import requests
from bs4 import BeautifulSoup

LG_URL = "https://www.iptp.net/iptp-tools/lg/"

def test_form_submission():
    print("Testing form submission to HCM router...")
    # These are typical LG parameters, we will check if the form uses them or something else.
    # First get the page to get any CSRF tokens if they exist
    session = requests.Session()
    session.verify = False 
    
    r = session.get(LG_URL)
    soup = BeautifulSoup(r.text, 'html.parser')
    
    form = soup.find('form')
    if not form:
        print("No form found!")
        return
        
    action = form.get('action', LG_URL)
    if not action.startswith('http'):
        action = "https://www.iptp.net" + action if action.startswith('/') else LG_URL + action
        
    print(f"Form action: {action}")
    
    inputs = form.find_all('input')
    data = {}
    for inp in inputs:
        name = inp.get('name')
        if name:
            data[name] = inp.get('value', '')
            
    print(f"Hidden/Default inputs: {data}")
    
    # Try guessing inputs based on common LGs:
    # We saw router names earlier
    selects = form.find_all('select')
    for select in selects:
        print(f"Select name: {select.get('name')}")
    
    textareas = form.find_all('textarea')
    for ta in textareas:
        print(f"Textarea name: {ta.get('name')}")

if __name__ == "__main__":
    test_form_submission()
