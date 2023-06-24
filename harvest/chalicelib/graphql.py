import logging
from datetime import datetime
from typing import Any

import requests  # type: ignore

from . import model
from . import timezone

logger = logging.getLogger(__name__)


class GraphQLClient:
    def __init__(self, endpoint: str, api_key: str):
        self.endpoint = endpoint
        self.api_key = api_key

    def list_reports(self, timestamp: int) -> list[model.RunReport]:
        """
        timestamp (unix time) 以降の周回報告を取得する
        """
        dt = datetime.fromtimestamp(timestamp, tz=timezone.Local)
        logger.info(f"since: {timestamp} ({dt})")

        headers = {
            "Content-Type": "application/graphql",
            "x-api-key": self.api_key,
        }
        query = """
query ListReports($nextToken: String, $timestamp: Int) {
    listReports(filter: { timestamp: {gt: $timestamp} }, nextToken: $nextToken) {
        items {
            id
            owner
            name
            twitterId
            twitterName
            twitterUsername
            type
            warName
            questType
            questName
            timestamp
            runs
            note
            createdAt
            dropObjects {
                objectName
                drops {
                    num
                    stack
                }
            }
        }
        nextToken
    }
}
        """

        reports = []
        next_token = None

        while True:
            resp = requests.post(
                self.endpoint,
                json={
                    "query": query,
                    "variables": {
                        "nextToken": next_token,
                        "timestamp": timestamp,
                    },
                },
                headers=headers,
            )

            if resp.status_code != 200:
                raise ValueError(f"Failed to fetch data from AppSync: {resp.text}")

            data = resp.json()
            items = data["data"]["listReports"]["items"]
            next_token = data["data"]["listReports"]["nextToken"]

            for item in items:
                report = self.to_report(item)
                reports.append(report)
            
            if not next_token:
                break

            logger.info("fetch next...")

        return reports

    def to_report(self, data: dict[str, Any]) -> model.RunReport:
        if data["twitterUsername"] is None:
            # twitter account "anonymous" (https://twitter.com/anonymous) は周回報告をしないと仮定してよい
            reporter = "anonymous"
        else:
            reporter = data["twitterUsername"]

        items = {}

        for drop in data["dropObjects"]:
            key = str(drop["objectName"])
            for num_with_stack in drop["drops"]:
                _num = num_with_stack["num"]
                if _num == -1:
                    num = "NaN"
                else:
                    num = str(_num)

                stack = num_with_stack["stack"]
                if stack == 1:
                    items[key] = num
                else:
                    if key == "QP":
                        plus_sign = True
                    elif key.endswith("ポイント"):
                        plus_sign = True
                    elif key.endswith("P"):
                        plus_sign = True
                    else:
                        plus_sign = False
                    
                    if plus_sign:
                        items[f"{key}(+{stack})"] = num
                    else:
                        items[f"{key}(x{stack})"] = num

        chapter = data["warName"]
        if chapter is None:
            chapter = ""

        place = data["questName"]
        if place is None:
            place = ""

        return model.RunReport(
            report_id=data["id"],
            tweet_id=None,
            reporter=reporter,
            reporter_id=data["owner"],
            chapter=chapter,
            place=place,
            runcount=data["runs"],
            items=items,
            note=data["note"],
            timestamp=_from_isoformat(data["createdAt"]),
            source="fgodrop",
        )


def _from_isoformat(datestr: str) -> datetime:
    # format: 2023-05-09T12:14:05.187Z
    if "." in datestr:
        t = datetime.fromisoformat(datestr.split(".")[0] + "+00:00")
    else:
        t = datetime.fromisoformat(datestr[:-1] + "+00:00")
    return t.astimezone(timezone.Local)
