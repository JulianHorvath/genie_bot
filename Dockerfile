FROM ubuntu:latest

RUN apt update -y && apt upgrade -y && apt install python -y && apt install python-pip -y && apt install git -y && apt autoremove -y

RUN git clone https://github.com/JulianHorvath/genie_bot.git

WORKDIR /genie_bot

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Make Streamlit listen on container network and set buffering
ENV PYTHONUNBUFFERED=1 

# Expose Streamlit default port
EXPOSE 8501

# Start Streamlit via CLI so we can pass server options
CMD ["streamlit", "run", "genie_bot.py", "--server.port=8501", "--server.address=0.0.0.0"]