import urllib.request

with urllib.request.urlopen("http://web/", timeout=30) as response:
    html = response.read().decode()
    assert response.status == 200
    assert '<div id="root"></div>' in html

with urllib.request.urlopen("http://web/api/health", timeout=30) as response:
    assert response.read().decode() == '{"status":"ok"}'

print("PASS frontend: SPA y proxy /api operativos")
