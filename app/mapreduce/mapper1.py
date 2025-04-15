import sys
import re

def tokenize(text):
    return re.findall(r"\w+", text.lower())

def main():
    for doc in sys.stdin:
        doc = doc.strip()
        id, title, text = doc.split("\t")
        tokens = tokenize(text)
        length = len(tokens)
        for token in tokens:
            print(f"{token}|{id}\t{id}<split>{title}<split>1<split>{length}")

if __name__ == '__main__':
    main()
