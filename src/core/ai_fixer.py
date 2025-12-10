import csv
import time
import re
from typing import List
from langchain_ollama import ChatOllama
from langchain_core.prompts import ChatPromptTemplate

class AIFixer:
    def __init__(self, model_name: str = "phi4-mini-reasoning:3.8b"):
        self.model_name = model_name
        self.llm = ChatOllama(model=model_name, temperature=0.1)
        
    def fix_ragged_row(self, header: List[str], bad_row_str: str) -> str:
        # Prompt explicitly asks for no markdown, but reasoning models often ignore this
        prompt = ChatPromptTemplate.from_template("""
        You are a Data Cleaning Expert.
        CONTEXT: Header ({num_cols} cols): {header}
        PROBLEM ROW: {bad_row}
        
        TASK: Fix the row to match the header column count. 
        - Merge split text fields (e.g. "NY, USA" -> "NY USA").
        - Fill missing fields with 'N/A'.
        - RETURN ONLY THE CSV ROW. DO NOT EXPLAIN.
        """)
        
        chain = prompt | self.llm
        
        try:
            time.sleep(0.1) 
            
            response = chain.invoke({
                "header": ",".join(header),
                "num_cols": len(header),
                "bad_row": bad_row_str
            })
            
            raw_content = response.content
            
            # --- CLEANING LOGIC ---
            # 1. Remove the <think>...</think> block
            clean_content = re.sub(r'<think>.*?</think>', '', raw_content, flags=re.DOTALL)
            
            # 2. Remove Markdown code blocks (```csv ... ```)
            clean_content = clean_content.replace("```csv", "").replace("```", "")
            
            # 3. Remove "Final Answer:" prefixes if the model adds them
            clean_content = re.sub(r'Final Answer:\s*', '', clean_content, flags=re.IGNORECASE)
            
            # 4. Remove LaTeX boxing \boxed{...} if present
            clean_content = clean_content.replace(r"\boxed{", "").replace("}", "")

            # 5. Get the last non-empty line (usually the actual data)
            lines = [line.strip() for line in clean_content.split('\n') if line.strip()]
            
            if not lines:
                return bad_row_str # Fallback if empty
                
            final_row = lines[-1] # Take the last line as the result
            
            return final_row
            
        except Exception as e:
            return f"AI Error: {str(e)}"