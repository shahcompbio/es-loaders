import requests
import os


class ColossusClient():

    __BASE_URL = "https://colossus.canadacentral.cloudapp.azure.com/api"

    def __init__(self):
        pass

    def get_analysis_information(self, jira_id):
        user = os.environ['COLOSSUS_API_USERNAME']
        password = os.environ['COLOSSUS_API_PASSWORD']
        response = requests.get(
            self.__BASE_URL + '/analysis_information/?analysis_jira_ticket=' + jira_id, auth=(user, password))

        return response.json()['results'][0]
