import json
import requests
import time

GITHUB_TOKEN = "token"
API_URL = "https://api.github.com/graphql"

# Function to get the top 400 repositories
def get_top_400_repos():
    query_template = """
    {
      search(query: "stars:>0 sort:stars-desc", type: REPOSITORY, first: 100, after: AFTER_CURSOR) {
        edges {
          node {
            ... on Repository {
              nameWithOwner
              stargazerCount
              url
            }
          }
        }
        pageInfo {
          endCursor
          hasNextPage
        }
      }
    }
    """

    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
    repos = []
    has_next_page = True
    cursor = None

    while has_next_page and len(repos) < 400:
        # Modify the query to include the cursor for pagination
        query = query_template.replace("AFTER_CURSOR", f'"{cursor}"' if cursor else "null")

        response = requests.post(API_URL, json={"query": query}, headers=headers)

        if response.status_code == 200:
            data = response.json()
            if "errors" in data:
                raise Exception(f"Error in GraphQL response: {data['errors']}")

            search_data = data["data"]["search"]
            repos.extend(search_data["edges"])

            has_next_page = search_data["pageInfo"]["hasNextPage"]
            cursor = search_data["pageInfo"]["endCursor"]

            # Introduce a small delay to avoid overloading the server
            time.sleep(1)

        elif response.status_code == 502:
            print("Received 502 timeout, retrying...")
            time.sleep(2)
        else:
            raise Exception(f"Query failed with status code {response.status_code}: {response.text}")

    return repos[:400]

# Function to get the first 300 pull requests from the top 400 repos
def get_pull_requests_for_repos(repos):
    query_template = """
    {
      repository(owner: "OWNER", name: "NAME") {
        pullRequests(states: [MERGED, CLOSED], first: 20, after: AFTER_CURSOR, orderBy: {field: CREATED_AT, direction: DESC}) {
          edges {
            node {
              title
              url
              state
              createdAt
              closedAt
              mergedAt
              reviews {
                totalCount
              }
              files(first: 0) {  # Get the number of files
                totalCount
              }
              additions  # Number of added lines
              deletions  # Number of removed lines
              body  # Description in markdown
              participants(first: 0) {  # Get the number of participants
                totalCount
              }
              comments(first: 0) {  # Get the number of comments
                totalCount
              }
            }
          }
          pageInfo {
            endCursor
            hasNextPage
          }
        }
      }
    }
    """

    headers = {"Authorization": f"Bearer {GITHUB_TOKEN}"}
    repo_pr_map = {}
    count = 0

    for repo in repos:
        repo_name_with_owner = repo['node']['nameWithOwner']
        owner, name = repo_name_with_owner.split('/')
        cursor = None
        has_next_page = True
        repo_pr_map[repo_name_with_owner] = []

        while has_next_page and len(repo_pr_map[repo_name_with_owner]) < 300:
            query = query_template.replace("OWNER", owner).replace("NAME", name).replace("AFTER_CURSOR", f'"{cursor}"' if cursor else "null")

            response = requests.post(API_URL, json={"query": query}, headers=headers)

            if response.status_code == 200:
                data = response.json()
                if "errors" in data:
                    raise Exception(f"Error in GraphQL response: {data['errors']}")

                pr_data = data["data"]["repository"]["pullRequests"]
                repo_pr_map[repo_name_with_owner].extend(pr_data["edges"])

                has_next_page = pr_data["pageInfo"]["hasNextPage"]
                cursor = pr_data["pageInfo"]["endCursor"]

                time.sleep(1)

            elif response.status_code == 502 or response.status_code == 504:
                print(f"Received 502/504 timeout for repo {repo_name_with_owner}, retrying...")
                time.sleep(2)
            else:
                raise Exception(f"Query failed with status code {response.status_code}: {response.text}")
        count += 1
        print(f"Processed {count} repositories.")

    return repo_pr_map

# Function to combine the repositories and PRs and save to a JSON file
def save_repos_and_prs_to_json(repos, repo_pr_map, json_filename):
    data = []

    # Combine repos and PRs into a structured format
    for repo in repos:
        repo_name_with_owner = repo['node']['nameWithOwner']
        repo_stars = repo['node']['stargazerCount']
        repo_url = repo['node']['url']

        pull_requests = repo_pr_map.get(repo_name_with_owner, [])

        repo_data = {
            'repository': {
                'nameWithOwner': repo_name_with_owner,
                'stars': repo_stars,
                'url': repo_url,
                'pullRequests': []
            }
        }

        for pr in pull_requests:
            pr_node = pr['node']
            pr_data = {
                'title': pr_node['title'],
                'url': pr_node['url'],
                'state': pr_node['state'],
                'createdAt': pr_node['createdAt'],
                'closedAt': pr_node['closedAt'],
                'mergedAt': pr_node['mergedAt'],
                'reviewCount': pr_node['reviews']['totalCount'],
                'numberOfFiles': pr_node['files']['totalCount'],  # Number of files changed
                'addedLines': pr_node['additions'],  # Number of added lines
                'removedLines': pr_node['deletions'],  # Number of removed lines
                'descriptionLength': len(pr_node['body']),  # Description length
                'numberOfParticipants': pr_node['participants']['totalCount'],  # Number of participants
                'numberOfComments': pr_node['comments']['totalCount'],  # Number of comments
            }
            repo_data['repository']['pullRequests'].append(pr_data)

        data.append(repo_data)

    with open(json_filename, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)


# Example usage
if __name__ == "__main__":
    # Fetch the top 400 repositories
    repos = get_top_400_repos()
    print(f"Fetched {len(repos)} repositories.")

    # Fetch the first 300 pull requests from these repositories
    repo_pr_map = get_pull_requests_for_repos(repos)
    print(f"Fetched pull requests for {len(repo_pr_map)} repositories.")

    # Save the repositories and their pull requests to a JSON file
    save_repos_and_prs_to_json(repos, repo_pr_map, "repos_and_prs.json")
    print("Data saved to repos_and_prs.json")
