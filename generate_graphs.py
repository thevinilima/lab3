import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Load the filtered PR data from the merged file
def load_pr_data(filename):
    with open(filename, 'r', encoding='utf-8') as f:
        data = json.load(f)

    # Flatten the data to create a dataframe
    pr_records = []
    for repo in data:
        for pr in repo['repository']['pullRequests']:
            pr_record = {
                'repository': repo['repository']['nameWithOwner'],
                'stars': repo['repository']['stars'],
                'title': pr['title'],
                'url': pr['url'],
                'createdAt': pr['createdAt'],
                'closedAt': pr['closedAt'],
                'mergedAt': pr['mergedAt'],
                'reviewCount': pr['reviewCount'],
                'numberOfFiles': pr.get('numberOfFiles', 0),
                'additions': pr.get('additions', 0),
                'deletions': pr.get('deletions', 0),
                'descriptionLength': pr['descriptionSize'],
                'participants': pr['participantsCount'],
                'comments': pr['commentsCount'],
                'state': pr['state']
            }
            pr_records.append(pr_record)

    return pd.DataFrame(pr_records)

# RQ 01-08 Analysis and Graph Generation
def generate_graphs(df):
    # Convert datetime strings to actual datetime objects
    df['createdAt'] = pd.to_datetime(df['createdAt'])
    df['closedAt'] = pd.to_datetime(df['closedAt'])
    df['mergedAt'] = pd.to_datetime(df['mergedAt'])

    # Calculate the time difference for analysis duration
    df['analysisTime'] = (df['closedAt'] - df['createdAt']).dt.total_seconds() / 3600.0  # Hours

    # Map PR states: "MERGED" -> 1 (positive), "CLOSED" -> 0 (negative)
    df['feedback'] = df['state'].map({'MERGED': 1, 'CLOSED': 0})

    # RQ 01: PR size (additions + deletions) vs feedback (MERGED or CLOSED)
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='additions', y='feedback', hue='feedback', palette='coolwarm', s=100)
    plt.title('RQ01: PR Size (Additions) vs Feedback (MERGED or CLOSED)')
    plt.xlabel('Number of Additions')
    plt.ylabel('Feedback (1 = MERGED, 0 = CLOSED)')
    plt.savefig('RQ01_PR_Size_vs_Feedback.png')  # Save graph as a file
    plt.close()

    # RQ 02: Analysis time vs feedback (MERGED or CLOSED)
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='analysisTime', y='feedback', hue='feedback', palette='coolwarm', s=100)
    plt.title('RQ02: PR Analysis Time vs Feedback (MERGED or CLOSED)')
    plt.xlabel('Analysis Time (hours)')
    plt.ylabel('Feedback (1 = MERGED, 0 = CLOSED)')
    plt.savefig('RQ02_Analysis_Time_vs_Feedback.png')  # Save graph as a file
    plt.close()

    # RQ 03: PR description length vs feedback (MERGED or CLOSED)
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='descriptionLength', y='feedback', hue='feedback', palette='coolwarm', s=100)
    plt.title('RQ03: PR Description Length vs Feedback (MERGED or CLOSED)')
    plt.xlabel('Description Length (characters)')
    plt.ylabel('Feedback (1 = MERGED, 0 = CLOSED)')
    plt.savefig('RQ03_Description_Length_vs_Feedback.png')  # Save graph as a file
    plt.close()

    # RQ 04: Interactions (comments + participants) vs feedback (MERGED or CLOSED)
    df['totalInteractions'] = df['comments'] + df['participants']
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='totalInteractions', y='feedback', hue='feedback', palette='coolwarm', s=100)
    plt.title('RQ04: PR Interactions (Comments + Participants) vs Feedback (MERGED or CLOSED)')
    plt.xlabel('Total Interactions')
    plt.ylabel('Feedback (1 = MERGED, 0 = CLOSED)')
    plt.savefig('RQ04_Interactions_vs_Feedback.png')  # Save graph as a file
    plt.close()

    # RQ 05: PR size vs number of revisions (approximated by reviewCount)
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='additions', y='reviewCount', hue='reviewCount', palette='coolwarm')
    plt.title('RQ05: PR Size (Additions) vs Number of Revisions (Review Count)')
    plt.xlabel('Number of Additions')
    plt.ylabel('Review Count')
    plt.savefig('RQ05_PR_Size_vs_Revisions.png')  # Save graph as a file
    plt.close()

    # RQ 06: PR analysis time vs number of revisions (approximated by reviewCount)
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='analysisTime', y='reviewCount', hue='reviewCount', palette='coolwarm')
    plt.title('RQ06: PR Analysis Time vs Number of Revisions (Review Count)')
    plt.xlabel('Analysis Time (hours)')
    plt.ylabel('Review Count')
    plt.savefig('RQ06_Analysis_Time_vs_Revisions.png')  # Save graph as a file
    plt.close()

    # RQ 07: PR description length vs number of revisions (reviewCount)
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='descriptionLength', y='reviewCount', hue='reviewCount', palette='coolwarm')
    plt.title('RQ07: PR Description Length vs Number of Revisions (Review Count)')
    plt.xlabel('Description Length (characters)')
    plt.ylabel('Review Count')
    plt.savefig('RQ07_Description_Length_vs_Revisions.png')  # Save graph as a file
    plt.close()

    # RQ 08: Interactions (comments + participants) vs number of revisions (reviewCount)
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='totalInteractions', y='reviewCount', hue='reviewCount', palette='coolwarm')
    plt.title('RQ08: PR Interactions (Comments + Participants) vs Number of Revisions (Review Count)')
    plt.xlabel('Total Interactions')
    plt.ylabel('Review Count')
    plt.savefig('RQ08_Interactions_vs_Revisions.png')  # Save graph as a file
    plt.close()

# Main script to load data and generate graphs
if __name__ == "__main__":
    # Load the PR data
    pr_data = load_pr_data("filtered_prs.json")

    # Generate graphs based on the research questions
    generate_graphs(pr_data)
