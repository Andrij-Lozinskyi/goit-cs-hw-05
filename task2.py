import requests
import re
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import matplotlib.pyplot as plt
from typing import List, Dict, Tuple

class MapReduce:
    def __init__(self, num_workers: int = 4):
        self.num_workers = num_workers
    
    def map_function(self, text: str) -> List[Tuple[str, int]]:
        words = re.findall(r'\b\w+\b', text.lower())
        return [(word, 1) for word in words if len(word) > 2]
    
    def reduce_function(self, mapped_data: List[Tuple[str, int]]) -> Dict[str, int]:
        word_counts = Counter()
        for word, count in mapped_data:
            word_counts[word] += count
        return dict(word_counts)
    
    def parallel_map(self, text: str) -> List[Tuple[str, int]]:
        chunk_size = max(len(text) // self.num_workers, 1)
        chunks = [text[i:i + chunk_size] for i in range(0, len(text), chunk_size)]
        
        with ThreadPoolExecutor(max_workers=self.num_workers) as executor:
            mapped_chunks = list(executor.map(self.map_function, chunks))
        
        return [item for sublist in mapped_chunks for item in sublist]

def fetch_text(url: str) -> str:
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        print(f"Помилка при завантаженні тексту: {e}")
        return ""

def visualize_top_words(word_counts: Dict[str, int], top_n: int = 10):
    top_words = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)[:top_n]
    words, counts = zip(*top_words)
    
    plt.figure(figsize=(12, 6))
    bars = plt.bar(range(len(words)), counts)
    
    plt.title('Топ слова за частотою використання')
    plt.xlabel('Слова')
    plt.ylabel('Частота')
    plt.xticks(range(len(words)), words, rotation=45, ha='right')
    
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}',
                ha='center', va='bottom')
    
    plt.tight_layout()
    plt.show()

def main():
    while True:
        url = input("Введіть URL для аналізу тексту (або 'q' для завершення): ")
        
        if url.lower() == 'q':
            print("Програма завершена.")
            break
        
        text = fetch_text(url)
        if not text:
            continue
        
        mapreduce = MapReduce(num_workers=4)
        mapped_data = mapreduce.parallel_map(text)
        word_frequencies = mapreduce.reduce_function(mapped_data)
        
        visualize_top_words(word_frequencies)

if __name__ == "__main__":
    main()
    