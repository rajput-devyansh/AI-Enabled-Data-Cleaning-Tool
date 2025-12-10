import csv
import time
from typing import List
from langchain_ollama import ChatOllama
from langchain_core.prompts import ChatPromptTemplate

class AIFixer:
    # UPDATED: Changed default model to 'phi4' (standard tag). 
    # If you specifically need the 3.8B variant, ensure you pulled 'phi3:mini' or check your tag.
    def __init__(self, model_name: str = "phi4-mini-reasoning:3.8b"):
        self.model_name = model_name
        self.llm = ChatOllama(model=model_name, temperature=0.1)
        
    def fix_ragged_row(self, header: List[str], bad_row_str: str) -> str:
        prompt = ChatPromptTemplate.from_template("""
        You are a Data Cleaning Expert.
        CONTEXT: Header ({num_cols} cols): {header}
        PROBLEM ROW: {bad_row}
        
        TASK: Fix the row to match the header column count. 
        - Merge split text fields (e.g. "NY, USA" -> "NY USA").
        - Fill missing fields with 'N/A'.
        - OUTPUT ONLY THE CSV ROW. NO MARKDOWN.
        """)
        
        chain = prompt | self.llm
        
        try:
            # Add a tiny delay to prevent overwhelming Ollama on loops
            time.sleep(0.1) 
            
            response = chain.invoke({
                "header": ",".join(header),
                "num_cols": len(header),
                "bad_row": bad_row_str
            })
            
            # Clean output
            return response.content.strip().replace("```csv", "").replace("```", "")
            
        except Exception as e:
            # Fallback for UI: Return the error so the user sees it in the table
            return f"AI Error: {str(e)}"