import pdfplumber
import re

def extract_text(pdf_path):
    text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            t = page.extract_text()
            if t:
                text += t + "\n"
    return text


def ai_extract(text):
    prompt = f"""
    Extract KYC fields from text.

    Return JSON:
    name, father_name, address, mobile, pan

    TEXT:
    {text}
    """

    # simulate — replace with real LLM call
    print(prompt)

def fallback(text):
    phones = re.findall(r"\b[6-9]\d{9}\b", text)
    pan = re.search(r"\b[A-Z]{5}[0-9]{4}[A-Z]\b", text)

    return {
        "mobile": phones,
        "pan": pan.group(0) if pan else None
    }
