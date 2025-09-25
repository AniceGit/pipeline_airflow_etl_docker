FROM python:3.10-slim

WORKDIR /app

COPY requirements_streamlit.txt .
RUN pip install --no-cache-dir -r requirements_streamlit.txt

COPY . /app

EXPOSE 8501
CMD ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
