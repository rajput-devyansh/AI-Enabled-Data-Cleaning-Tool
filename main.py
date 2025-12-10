import os
import sys
from streamlit.web import cli as stcli

def main():
    """
    Launches the Streamlit Application.
    """
    sys.argv = ["streamlit", "run", "src/ui/app.py"]
    sys.exit(stcli.main())

if __name__ == "__main__":
    main()