
import json
from iranseda.details import get_mp3s_from_api

def test_api_parsing_monkeypatch(monkeypatch):
    class Resp:
        status_code = 200
        def json(self): return {"items": [{"FileID": 1, "download": [{"extension": "mp3", "downloadUrl": "https://player.iranseda.ir/downloadfile/?attid=111&q=11", "fileSize": 12345, "bitRate": 64}, {"extension": "mp3", "downloadUrl": "https://player.iranseda.ir/downloadfile/?attid=111&q=12", "fileSize": 22345, "bitRate": 128}]}]}
    monkeypatch.setattr("iranseda.details.req_get", lambda url: Resp())
    mp3s = get_mp3s_from_api(1, 1)
    assert len(mp3s) == 2
    assert mp3s[0]["url"].startswith("https://")
