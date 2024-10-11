from azure.storage.blob import ContainerClient
from io import BytesIO
from pdfminer.high_level import extract_text
import requests
import sys
import json
import re

# Azure Blob Storage configuration
storage_connection_string = "Enter Your Connection String"
container_name = "Enter the Name of Container where the file is located"

# Azure OpenAI configuration
openai_endpoint = "Enter your Endpoint"
openai_api_key = "Enter Your Api Key"
deployment_name = "gpt-4o"
api_version = "2023-03-15-preview"
api_url = f"{openai_endpoint}openai/deployments/{deployment_name}/chat/completions?api-version={api_version}"

headers = {
    "Content-Type": "application/json",
    "api-key": openai_api_key
}

def send_to_openai(prompt_text):
    payload = {
        "messages": [
            {"role": "system", "content": '''You are an AI assistant. I'm giving you a contract.  What I need you to do is exactly parse out each section and put it into a JSON format  and If the section is long, summarize key points to fit within the token limit. Here's the format to follow:  
            {
              "name": "name of company",
              "effective_date": "date the contract goes into effect",
              "sections": {
                "clause_number": "2(b)",
                "clause_title": "Renewal Term",
                "task_description": "Defines the term of the agreement, including the initial term and renewal terms.",
                "frequency": "per contract sixty (60) days",
                "category": "task category",
                "clause_text": ["text"]
              }
            }'''},
            {"role": "user", "content": prompt_text}
        ],
        "max_tokens": 4096,
        "temperature": 0.2,
        "top_p": 0.95,
        "frequency_penalty": 0,
        "presence_penalty": 0,
    }

    response = requests.post(api_url, headers=headers, data=json.dumps(payload))
    if response.status_code == 200:
        result = response.json()
        reply = result['choices'][0]['message']['content']
        return reply
    else:
        print(f"Request failed with status code {response.status_code}: {response.text}")
        return None

def split_sections(text):
    pattern = r'(?<=\n)(\d+\.\s+|\nSection\s+[A-Z]+\s*:\s*)'
    sections = re.split(pattern, text)
    return [(sections[i].strip(), sections[i + 1].strip()) for i in range(1, len(sections), 2)]

def main():
    container_client = ContainerClient.from_connection_string(
        conn_str=storage_connection_string,
        container_name=container_name
    )

    blob_list = container_client.list_blobs()

    for blob in blob_list:
        print(f"Processing blob: {blob.name}")
        blob_client = container_client.get_blob_client(blob)
        downloader = blob_client.download_blob()
        blob_content = downloader.readall()

        content_type = blob_client.get_blob_properties().content_settings.content_type
        print(f"Content type of {blob.name}: {content_type}")

        if content_type == 'application/pdf' or blob.name.lower().endswith('.pdf'):
            try:
                file_stream = BytesIO(blob_content)
                text = extract_text(file_stream)
                blob_text = text.strip()
                print("Done processing PDF.")

            except Exception as e:
                print(f"Error processing PDF {blob.name}: {e}")
                continue
        else:
            print(f"Blob {blob.name} is not a PDF. Skipping.")
            continue

        print(f"Size of blob is: {len(blob_text)}")

        sections = split_sections(blob_text)
        for section_title, section_content in sections:
           # print(f"Processing section: {section_title}")
            response = send_to_openai(section_content)
            if response:
                #print(f"Response for section: {section_title}\n\n {response}\n\n")
                with open("output.txt", "a") as f:
                    f.write(f"{section_title}\n{response}\n\n")

if __name__ == "__main__":
    main()
