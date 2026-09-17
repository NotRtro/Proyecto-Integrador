"""Run with the web image: python /data/scripts/test_web.py."""
from pathlib import Path
from streamlit.testing.v1 import AppTest

for page in [Path('/workspace/app/Home.py'),*sorted(Path('/workspace/app/pages').glob('*.py'))]:
    app=AppTest.from_file(str(page),default_timeout=60).run()
    assert not app.exception, f'{page.name}: {[e.message for e in app.exception]}'
    print(f'PASS {page.name}',flush=True)
    # Exercise the first alternate option of each initial selectbox.
    for i in range(len(app.selectbox)):
        if i>=len(app.selectbox): break
        control=app.selectbox[i]
        if len(control.options)>1:
            control.select_index(1).run()
            assert not app.exception, f'{page.name}, {control.label}: {[e.message for e in app.exception]}'
    print(f'PASS controls {page.name}',flush=True)
