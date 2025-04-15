import sys
import re

def tokenize(text):
    return re.findall(r"\w+", text.lower())

def main():
    for doc in sys.stdin:
        doc = doc.strip()
        id, title, text = doc.split("\t")
        tokens = tokenize(text)
        for token in tokens:
            print(f"{token}\t{id},1")

if __name__ == '__main__':
    main()
