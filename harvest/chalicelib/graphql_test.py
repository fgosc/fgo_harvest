from datetime import datetime

from . import graphql


def test_to_report():
    client = graphql.GraphQLClient('', '')
    data = {
        "id": "abcd1234-abcd-1234-5678-000011112222",
        "owner": "abcd1234-abcd-1234-5678-12345678abcd",
        "name": "名無しさん",
        "twitterId": "12341234123412341234",
        "twitterName": "名無しさん",
        "twitterUsername": "someuser",
        "type": "open",
        "warName": "オーディール・コール",
        "questType": "normal",
        "questName": "ハワイエリア",
        "timestamp": 1687059798,
        "runs": 3,
        "note": "貝殻泥UP %\n羽根泥UP %",
        "createdAt": "2023-06-18T03:43:15.239Z",
        "dropObjects": [
            {
                "objectName": "貝殻",
                "drops": [
                    {
                    "num": 5,
                    "stack": 1
                    }
                ]
            },
            {
                "objectName": "羽根",
                "drops": [
                    {
                    "num": -1,
                    "stack": 1
                    }
                ]
            }
        ]
    }
    report = client.to_report(data)

    assert report.report_id == "abcd1234-abcd-1234-5678-000011112222"
    assert report.tweet_id is None
    assert report.get_id() == "abcd1234-abcd-1234-5678-000011112222"
    assert report.reporter == "someuser"
    assert report.reporter_id == "abcd1234-abcd-1234-5678-12345678abcd"
    assert report.chapter == "オーディール・コール"
    assert report.place == "ハワイエリア"
    assert report.runcount == 3
    assert report.items == {
        "貝殻": "5",
        "羽根": "NaN",
    }
    assert report.note == "貝殻泥UP %\n羽根泥UP %"
    assert report.timestamp == datetime.fromisoformat("2023-06-18T12:43:15+09:00")
    assert report.source == "fgodrop"
