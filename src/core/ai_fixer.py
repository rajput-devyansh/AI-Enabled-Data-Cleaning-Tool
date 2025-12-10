import csv
import io
from typing import List, Dict
from langchain_ollama import ChatOllama
from langchain_core.prompts import ChatPromptTemplate

class AIFixer:
    def __init__(self, model_name: str = "phi4"):
        # We use a temperature of 0.1 for deterministic, precise fixes
        self.llm = ChatOllama(model=model_name, temperature=0.1)
        
    def fix_ragged_row(self, header: List[str], bad_row_str: str) -> str:
        """
        Asks the AI to align a broken row with the header.
        """
        prompt = ChatPromptTemplate.from_template("""
        You are a Data Cleaning Expert.
        
        CONTEXT:
        A CSV file has a header with {num_cols} columns: {header}
        
        PROBLEM:
        This row is malformed (ragged): 
        {bad_row}
        
        INSTRUCTION:
        Fix the row to have exactly {num_cols} columns that match the header meaning.
        - If a field is missing, insert 'N/A'.
        - If a field is split by an extra comma (e.g. "Paris, France"), merge it (e.g. "Paris France").
        - Output ONLY the corrected CSV row. No markdown, no explanations.
        """)
        
        chain = prompt | self.llm
        
        try:
            response = chain.invoke({
                "header": ",".join(header),
                "num_cols": len(header),
                "bad_row": bad_row_str
            })
            # Clean up potential markdown formatting from AI
            cleaned_content = response.content.strip().replace("```csv", "").replace("```", "")
            return cleaned_content
        except Exception as e:
            return f"ERROR: {str(e)}"

    def batch_fix(self, header: List[str], bad_rows: List[str]) -> List[str]:
        """
        Iterates through a list of bad rows and fixes them.
        (For production, we would batch this, but loop is safer for local LLMs to avoid context limits)
        """
        fixed_rows = []
        for row in bad_rows:
            if not row.strip(): continue
            fixed = self.fix_ragged_row(header, row)
            fixed_rows.append(fixed)
        return fixed_rows