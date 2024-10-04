import json
from datetime import datetime, timedelta

# Function to filter pull requests and repositories based on specified conditions
def filter_pull_requests(input_filename, output_filename):
    with open(input_filename, 'r', encoding='utf-8') as f:
        data = json.load(f)

    filtered_data = []

    for repo in data:
        repo_name_with_owner = repo['repository']['nameWithOwner']
        repo_stars = repo['repository']['stars']
        repo_url = repo['repository']['url']

        # Filtered pull requests for this repository
        filtered_pull_requests = []

        for pr in repo['repository']['pullRequests']:
            created_at = datetime.fromisoformat(pr['createdAt'].replace("Z", "+00:00"))
            closed_at = datetime.fromisoformat(pr['closedAt'].replace("Z", "+00:00")) if pr['closedAt'] else None
            merged_at = datetime.fromisoformat(pr['mergedAt'].replace("Z", "+00:00")) if pr['mergedAt'] else None

            # Check if the PR has at least one review
            has_reviews = pr['reviewCount'] > 0

            # Calculate the time difference
            time_diff = None
            if merged_at:
                time_diff = merged_at - created_at
            elif closed_at:
                time_diff = closed_at - created_at

            # Check the time difference and review conditions
            if has_reviews and time_diff and time_diff >= timedelta(hours=1):
                pr['descriptionSize'] = len(pr['body'])
                del pr['body']
                filtered_pull_requests.append(pr)

        # Only include repos with 100 or more filtered PRs
        if len(filtered_pull_requests) >= 100:
            filtered_repo_data = {
                'repository': {
                    'nameWithOwner': repo_name_with_owner,
                    'stars': repo_stars,
                    'url': repo_url,
                    'pullRequests': filtered_pull_requests
                }
            }
            filtered_data.append(filtered_repo_data)

    # Save filtered data to a new JSON file
    with open(output_filename, 'w', encoding='utf-8') as f:
        json.dump(filtered_data, f, indent=4)

# Example usage
if __name__ == "__main__":
    filter_pull_requests("repos_and_prs.json", "filtered_prs.json")
    print("Filtered data saved to filtered_prs.json")
