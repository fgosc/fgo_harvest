from . import model


def test_retrieve_runreport():
    data = {
        "id": 1495032114890559488,
        "tweet_id": 1495032114890559488,
        "report_id": None,
        "timestamp": "2022-02-19T22:46:47+09:00",
        "reporter": "_8_LotuS_8_",
        "reporter_id": None,
        "chapter": "町への脅威を取り除け",
        "place": "",
        "runcount": 100,
        "items": {
            "礼装": "3",
            "胆石": "28",
            "冠": "54",
            "術秘": "33",
            "術魔": "20",
            "術モ": "34",
            "ショコラトル(x3)": "911",
            "パウダー(x3)": "915",
            "カカオチップ(x3)": "927",
        },
        "note": "",
        "quest_id": "QevNqjrdjveF",
        "source": "twitter",
        "freequest": False,
    }
    report = model.RunReport.retrieve(data)

    assert report.tweet_id == data["id"]
    assert report.reporter == data["reporter"]
    assert report.chapter == data["chapter"]
    assert report.place == data["place"]
    assert report.runcount == data["runcount"]
    assert report.timestamp.isoformat() == data["timestamp"]
    assert report.is_freequest == data["freequest"]
    assert report.quest_id == data["quest_id"]
    assert report.items == data["items"]
