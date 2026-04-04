# import pdfplumber
# import re

# def extract_text(path):
#     text = ""
#     with pdfplumber.open(path) as pdf:
#         for page in pdf.pages:
#             text += page.extract_text() + "\n"
#     return text

# def extract_kyc_fields(text):
#     name = re.search(r"Name:\s*(.*)", text)
#     pan = re.search(r"[A-Z]{5}[0-9]{4}[A-Z]", text)
#     phones = re.findall(r"\b[6-9]\d{9}\b", text)
#     mobile = phones[0] if phones else None

#     return {
#         "name": name.group(1).strip() if name else None,
#         "pan": pan.group(0) if pan else None,
#         "mobile": mobile
#     }

import pdfplumber
import re

def extract_text(path):
    text = ""
    with pdfplumber.open(path) as pdf:
        for page in pdf.pages:
            extracted = page.extract_text()
            if extracted:
                text += extracted + "\n"
    return text

def extract_name(text):
    patterns = [
        r"Customer.?Name[:\s]*(.*)",
        r"Name\*?[:\s]*(.*)",
        r"Applicant.?Name[:\s]*(.*)"
    ]

    for pattern in patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            name = match.group(1).strip()
            if len(name) > 2:
                return name

    return None

def extract_kyc_fields(text):

    name = extract_name(text)

    pan_match = re.search(r"\b[A-Z]{5}[0-9]{4}[A-Z]\b", text)

    phones = re.findall(r"\b[6-9]\d{9}\b", text)

    return {
        "name": name,
        "pan": pan_match.group(0) if pan_match else None,
        "mobile": phones[0] if phones else None,
        "all_phones": phones
    }
