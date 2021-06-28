from gql import gql, Client
from gql.transport.requests import RequestsHTTPTransport
import time

class Dune:
    def __init__(self, username=None, password=None, transport=None):
        
        if transport is None:
            transport = RequestsHTTPTransport(url="https://core-hsr.duneanalytics.com/v1/graphql")

        self.client = Client(transport=transport, fetch_schema_from_transport=True)

        if username and password:
            self.authenticate(username, password)

    def authenticate(self, username, password):
        transport = self.client.transport

        csrf = transport.session.post("https://duneanalytics.com/api/auth/csrf").json()['csrf']
        r_auth = transport.session.post("https://duneanalytics.com/api/auth", data={
            "csrf": csrf,
            "action": "login",
            "username": username,
            "password": password
        }, headers={
            "Origin": "https://duneanalytics.com",
            "Referer": "https://duneanalytics.com/auth/login"
        }, allow_redirects=False)

        self.refresh_session()
    
    def refresh_session(self):
        r_session = self.client.transport.session.post("https://duneanalytics.com/api/auth/session")
        dune_session = r_session.json()

        if 'token' in dune_session:
            self.sub = dune_session['sub']
            self.client.transport.session.headers['Authorization'] = "Bearer " + dune_session['token']

            self.user = self.find_session_user()

    def find_session_user(self):
        gql_query = gql("""
            query FindSessionUser($sub: uuid!) {
                users(where: {
                    private_info: {
                        cognito_id: {_eq: $sub}
                    }
                }) { ...SessionUser    __typename  }
            }
            fragment SessionUser on users {
                id  name  profile_image_url  memberships {
                    group { ...Group      __typename    }
                    __typename
                }  __typename
            }
            fragment Group on groups {  id  type  permissions  __typename }
        """
        )

        result = self.client.execute(gql_query, operation_name="FindSessionUser", variable_values={"sub": self.sub})
        return result['users'][0]

    def query(self, query):
        self.refresh_session()
        new_query = self.upsert_query(query)

        job_id = self.execute_query(new_query['id'])
        self.wait_for_job(job_id)

        return self.find_result_data_by_job(job_id)

    def upsert_query(self, query):
        gql_query = gql("""
            mutation UpsertQuery($session_id: Int!, $object: queries_insert_input!, $on_conflict: queries_on_conflict!, $favs_last_24h: Boolean! = false, $favs_last_7d: Boolean! = false, $favs_last_30d: Boolean! = false, $favs_all_time: Boolean! = true) {
                insert_queries_one(object: $object, on_conflict: $on_conflict) {
                    ...Query
                    favorite_queries(where: {user_id: {_eq: $session_id}}, limit: 1) {
                        created_at
                        __typename
                    }
                    __typename
                }
            }

            fragment Query on queries {
                id
                dataset_id
                name
                description
                query
                private_to_group_id
                is_temp
                is_archived
                created_at
                updated_at
                schedule
                tags
                parameters
                user {
                    ...User
                    __typename
                }
                visualizations {
                    id
                    type
                    name
                    options
                    created_at
                    __typename
                }
                favorite_queries_aggregate @include(if: $favs_all_time) {
                    aggregate {
                    count
                    __typename
                    }
                    __typename
                }
                query_favorite_count_last_24h @include(if: $favs_last_24h) {
                    favorite_count
                    __typename
                }
                query_favorite_count_last_7d @include(if: $favs_last_7d) {
                    favorite_count
                    __typename
                }
                query_favorite_count_last_30d @include(if: $favs_last_30d) {
                    favorite_count
                    __typename
                }
                __typename
            }

            fragment User on users {
                id
                name
                profile_image_url
                __typename
            }
        """
        )

        result = self.client.execute(gql_query, operation_name="UpsertQuery", variable_values={
            "favs_last_24h": False,
            "favs_last_7d": False,
            "favs_last_30d": False,
            "favs_all_time": False,
            "object": {
            "schedule": None,
            "dataset_id": 4,
            "name": "Jupyter Test",
            "query": query,
            "user_id": self.user['id'],
            "description": "",
            "is_archived": False,
            "is_temp": True,
            "parameters": [],
            "visualizations": {
                "data": [
                {
                    "type": "table",
                    "name": "Query results",
                    "options": {}
                }
                ],
                "on_conflict": {
                "constraint": "visualizations_pkey",
                "update_columns": [
                    "name",
                    "options"
                ]
                }
            }
            },
            "on_conflict": {
            "constraint": "queries_pkey",
            "update_columns": [
                "dataset_id",
                "name",
                "description",
                "query",
                "schedule",
                "is_archived",
                "is_temp",
                "tags",
                "parameters"
            ]
            },
            "session_id": self.user['id']
        })

        return result['insert_queries_one']

    def execute_query(self, query_id, **kwargs):
        gql_query = gql("""
            mutation ExecuteQuery($query_id: Int!, $parameters: [Parameter!]!) {
                execute_query(query_id: $query_id, parameters: $parameters) {
                    job_id
                    __typename
                }
            }
        """)

        result = self.client.execute(gql_query, operation_name="ExecuteQuery", variable_values={
            "query_id": query_id,
            "parameters": [ { "key": key, "type": "text", "value": value} for key, value in kwargs.items() ]
        })

        return result['execute_query']['job_id']

    def find_result_job(self, job_id):
        gql_query = gql("""
            query FindResultJob($job_id: uuid) {
                jobs(where: {id: {_eq: $job_id}}) {
                    id
                    user_id
                    locked_until
                    created_at
                    category
                    __typename
                }
                view_queue_positions(where: {id: {_eq: $job_id}}) {
                    pos
                    __typename
                }
            }
        """)

        result = self.client.execute(gql_query, operation_name="FindResultJob", variable_values={
            "job_id": job_id
        })

        return result['jobs']

    def wait_for_job(self, job_id):
        while True:
            jobs = self.find_result_job(job_id)
            if len(jobs) == 0:
                break
            else:
                time.sleep(1)

    def find_result_data_by_job(self, job_id):
        gql_query = gql("""
            query FindResultDataByJob($job_id: uuid!) {
                query_results(where: {job_id: {_eq: $job_id}}) {
                    id
                    job_id
                    error
                    runtime
                    generated_at
                    columns
                    __typename
                }
                get_result_by_job_id(args: {want_job_id: $job_id}) {
                    data
                    __typename
                }
            }
        """)

        result = self.client.execute(gql_query, operation_name="FindResultDataByJob", variable_values={
            "job_id": job_id
        })

        for item in result['get_result_by_job_id']:
            yield item['data']
