FROM python:3.11-bullseye

# Open port 7860 for http service
ENV FAST_API_PORT=7860
EXPOSE 7860

# Install Python dependencies
COPY *.py .
COPY ./requirements.txt requirements.txt
RUN pip3 install --no-cache-dir --upgrade -r requirements.txt

# Install models
RUN python3 install_deps.py

# Start the FastAPI server
CMD python3 bot_runner.py --port ${FAST_API_PORT}