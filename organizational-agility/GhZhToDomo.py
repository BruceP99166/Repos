# Get issue data from Zenhub/Github

# Packages
import datetime
import logging
import requests
import json
import os
import sys
from dotenv import load_dotenv
from pydomo import Domo
from pydomo.datasets import DataSetRequest, Schema, Column, ColumnType
#from pydomo.datasets import Policy, PolicyFilter, FilterOperator, PolicyType, Sorting

# Initialize variables
domo_api_host = 'api.domo.com'
dsr = DataSetRequest()
json_gh_repos_filename = 'C:\\Users\\Bruce Pike Rice\\Downloads\\GhzhToDomo_GhIssues.json'
json_zh_repos_filename = 'C:\\Users\\Bruce Pike Rice\\Downloads\\GhzhToDomo_ZhIssues.json'
json_zh_releases_filename = 'C:\\Users\\Bruce Pike Rice\\Downloads\\GhzhToDomo_ZhReleases.json'
repo_list = [
#    ('openstax/tutor', '150623529'),
    ('openstax/work-management-reports', '107729911')
]  # '***USER***/***REPO***', '***repoID***
rows_repos = []
rows_releases = []
update_datetime = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")

# Environment variables
config_path = sys.argv[1]
load_dotenv(dotenv_path=config_path)

try:
    auth_github = ('token', os.environ['GITHUB_TOKEN'])
except:
    print("There is a problem with the GITHUB_TOKEN environment variable.")
    exit(1)

try:
    auth_zenhub = '?access_token=' + os.environ['ZENHUB_TOKEN']
except:
    print("There is a problem with the ZENHUB_TOKEN environment variable.")
    exit(1)

try:
    domo_client_id = (os.environ['DOMO_CLIENT_ID'])
except:
    print("There is a problem with the DOMO_CLIENT_ID environment variable.")
    exit(1)

try:
    domo_client_secret = (os.environ['DOMO_CLIENT_SECRET'])
except:
    print("There is a problem with the DOMO_CLIENT_SECRET environment variable.")
    exit(1)

try:
    ghzh_repo_history_dsid = (os.environ['GHZH_REPO_HISTORY_DSID'])
except:
    print("GHZH_REPO_HISTORY_DSID not found.")
    exit(1)

try:
    ghzh_release_history_dsid = (os.environ['GHZH_RELEASE_HISTORY_DSID'])
except:
    print("GHZH_RELEASE_HISTORY_DSID not found.")
    exit(1)

# Domo configure the logger
handler = logging.StreamHandler()
handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logging.getLogger().addHandler(handler)

# Domo: Create an instance of the Domo SDK Client
domo = Domo(domo_client_id, domo_client_secret, logger_name='domo_dataset',
            log_level=logging.INFO, api_host=domo_api_host)

def create_rows_for_issues_in_repo(r, repo_name, repo_id):
    estimate_value = 0
    pipeline_name = str('')

    if not r.status_code == 200:
        raise Exception(r.status_code)

    issues_in_repo_json = r.json()
    for issue in issues_in_repo_json:
        print(repo_name + ' issue Number: ' + str(issue['number']))

        # Get response from zenhub
        zh_issue_url = 'https://api.zenhub.io/p1/repositories/' + str(repo_id) + '/issues/' + str(
            issue['number']) + auth_zenhub
        zh_issues_json = requests.get(zh_issue_url).json()
        json.dump(zh_issues_json, json_zh_repos_file)

        if 'pull_request' not in issue:
            issue_labels = ''
            issue_milestones = ''
            issue_assignees = ''
            issue_epics = ''
            issue_releases = ''
            for x in issue['labels'] if issue['labels'] else []:
                issue_labels += x['name'] + ';'
            for x in issue['milestones'] if issue['milestones'] else []:
                issue_milestones += x['name'] + ';'
            for x in issue['assignees'] if issue['assignees'] else []:
                issue_assignees += x['name'] + ';'
            for x in issue['epics'] if issue['epics'] else []:
                issue_epics += x['name'] + ';'
            estimate_value = zh_issues_json.get('estimate', dict()).get('value', "")
            pipeline_name = zh_issues_json.get('pipeline', dict()).get('name', "")

            fields = [
                repo_name, 
                pipeline_name, 
                issue['title'].replace(',', ' '),
                str(issue['number']), 
                issue_labels,
                issue_milestones,
                issue_assignees,
                issue_epics,
                issue['url'], 
                "closed" if issue['closed_at'] else "open",
                str(estimate_value),
                repo_id,
                issue['node_id'],
                "0",
                update_datetime
            ]

            rows_repos.append(",".join(fields))
            a = 1


def get_issues_for_repo(repo_data):
    repo_name = repo_data[0]
    repo_id = repo_data[1]
    issues_for_repo_url = 'https://api.github.com/repos/%s/issues' % repo_name
    r = requests.get(issues_for_repo_url, auth=auth_github)
    json.dump(r.json(), json_gh_repos_file)
    create_rows_for_issues_in_repo(r, repo_name, repo_id)
    # more pages? examine the 'link' header returned
    if 'link' in r.headers:
        pages = dict(
            [(rel[6:-1], url[url.index('<') + 1:-1]) for url, rel in
             [link.split(';') for link in
              r.headers['link'].split(',')]])
        while 'last' in pages and 'next' in pages:
            pages = dict(
                [(rel[6:-1], url[url.index('<') + 1:-1]) for url, rel in
                 [link.split(';') for link in
                  r.headers['link'].split(',')]])
            r = requests.get(pages['next'], auth=auth_github)
            create_rows_for_issues_in_repo(r, repo_name, repo_id)
            if pages['next'] == pages['last']:
                break


def create_rows_for_issues_in_release(r, release):
    if not r.status_code == 200:
        raise Exception(r.status_code)

    issues_in_release_json = r.json()
    for issue in issues_in_release_json:
        print('Release ' + release["title"] + ' repo_id: ' + str(issue['repo_id']) + ' issue Number: ' + str(issue['issue_number']))

        if 'pull_request' not in issue:
            fields = [
                str(issue["repo_id"]),
                str(issue["issue_number"]),
                release["title"],
                "0",
                update_datetime
            ]

            rows_releases.append(",".join(fields))
            a = 1


def get_issues_for_releases(repo_data):
    repo_name = repo_data[0]
    repo_id = repo_data[1]

    # Get releases for repo
    request_url = 'https://api.zenhub.io/p1/repositories/' + str(repo_id) + '/reports/releases' + auth_zenhub
    zh_releases_for_repo_json = requests.get(request_url).json()

    # Get issues for release
    for release in zh_releases_for_repo_json:
        request_url = 'https://api.zenhub.io/p1/reports/release/' + str(release['release_id']) + '/issues' + auth_zenhub
        r = requests.get(request_url)
        # json.dump(r.json(), json_issues_for_release_file)
        create_rows_for_issues_in_release(r , release)
        # more pages? examine the 'link' header returned
        if 'link' in r.headers:
            pages = dict(
                [(rel[6:-1], url[url.index('<') + 1:-1]) for url, rel in
                 [link.split(';') for link in
                  r.headers['link'].split(',')]])
            while 'last' in pages and 'next' in pages:
                pages = dict(
                    [(rel[6:-1], url[url.index('<') + 1:-1]) for url, rel in
                     [link.split(';') for link in
                      r.headers['link'].split(',')]])
                r = requests.get(pages['next'], auth=auth_github)
                create_rows_for_issues_in_release(r , release)
                if pages['next'] == pages['last']:
                    break


##############################
#  Get repos data
##############################
# Open json output files
json_gh_repos_file = open(json_gh_repos_filename, 'w')
json_zh_repos_file = open(json_zh_repos_filename, 'w')

#  Get issue data from github and zenhub
for repo_data in repo_list:
    get_issues_for_repo(repo_data)

# Close json output files
json_gh_repos_file.close()
json_zh_repos_file.close()

# Domo Create ghzh_repos_history dataset
if ghzh_repo_history_dsid != "":
    ds_id = ghzh_repo_history_dsid
else:
    domo.logger.info("\n**** Create Domo dataset ghzh_repos_history ****\n")

    dsr.name = "ghzh_repos_history"
    dsr.description = ""
    dsr.schema = Schema(
        [
        Column(ColumnType.STRING, 'RepoName'),
        Column(ColumnType.STRING, 'PipelineName'),
        Column(ColumnType.STRING, 'IssueName'),
        Column(ColumnType.DECIMAL, 'IssueNumber'),
        Column(ColumnType.STRING, 'IssueLabels'),
        Column(ColumnType.STRING, 'IssueMilestones'),
        Column(ColumnType.STRING, 'IssueAssignees'),
        Column(ColumnType.STRING, 'IssueEpics'),
        Column(ColumnType.STRING, 'IssueUrl'),
        Column(ColumnType.STRING, 'IssueOpenClosed'),
        Column(ColumnType.DECIMAL, 'IssuePoints'),
        Column(ColumnType.STRING, 'RepoId'),
        Column(ColumnType.STRING, 'IssueId'),
        Column(ColumnType.DECIMAL, '_BATCH_ID_'),
        Column(ColumnType.DATETIME, '_BATCH_LAST_RUN_')
        ])

    dataset = domo.datasets.create(dsr)
    domo.logger.info("Created DataSet ghzh_repos_history id=" + dataset['id'])
    ds_id = dataset['id']

# Write repo data to Domo ds
#for row_repos in rows_repos:
#    domo.datasets.data_import(ds_id, row_repos, "APPEND")

#domo.logger.info("Appended today\'s data to DataSet ghzh_repos_history id= " + ds_id)

##############################
#  Get release data
##############################
# Open json output file
json_zh_releases_file = open(json_zh_releases_filename, 'w')

#  Get release data from zenhub
for repo_data in repo_list:
    get_issues_for_releases(repo_data)

# Close json output file
json_zh_releases_file.close()


# Domo Create ghzh_releases_history dataset
domo.logger.info("\n**** Create Domo dataset ghzh_releases_history ****\n")

dsr.name = "ghzh_releases_history"
dsr.description = ""
dsr.schema = Schema(
    [
    Column(ColumnType.STRING, 'RepoId'),
    Column(ColumnType.DECIMAL, 'IssueNumber'),
    Column(ColumnType.STRING, 'ReleaseTitle'),
    Column(ColumnType.DECIMAL, '_BATCH_ID_'),
    Column(ColumnType.DATETIME, '_BATCH_LAST_RUN_')
    ])

dataset = domo.datasets.create(dsr)
domo.logger.info("Created DataSet ghzh_releases_history id=" + dataset['id'])

# Write release data to Domo ds
ds_id = dataset['id']
for row_releases in rows_releases:
    domo.datasets.data_import(ds_id, row_releases, "APPEND")

domo.logger.info("Appended today\'s data to DataSet ghzh_releases_history id=" + ds_id)

