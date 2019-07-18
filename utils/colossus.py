import requests


class ColossusClient():

    __BASE_URL = "https://colossus.canadacentral.cloudapp.azure.com/api"

    def __init__(self):
        pass

    def get_analysis_information(self, jira_id, user, password):
        response = requests.get(
            self.__BASE_URL + '/analysis_information/?analysis_jira_ticket=' + jira_id, auth=(user, password))

        return response.json()['results']

    def get_all_analyses_information(self, user, password):

        response = requests.get(
            self.__BASE_URL + '/analysis_information/?no_pagination', auth=(user, password))
        return response.json()['results']
