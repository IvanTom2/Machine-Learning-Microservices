import requests

data = requests.get(
    "https://www.flashscore.co.uk/match/l81rCxw4/#/match-summary/match-summary"
)
print(data.text)
