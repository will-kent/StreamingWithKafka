FROM jupyter/pyspark-notebook:8d32a5208ca1 as build

RUN pip install pandas==1.2.2 \
    && pip install pyarrow==2.0.0 \
    && pip install confluent-kafka==1.6.1 \
    && pip install Faker==7.0.1 \
    && pip install sseclient==0.0.27 \
    && pip install praw==7.2.0 \
    && pip install configparser==5.0.1

ENV PYTHONPATH "${PYTHONPATH}:/home/jovyan/work"

RUN echo "export PYTHONPATH=/home/jovyan/work" >> ~/.bashrc

WORKDIR /home/jovyan/work

CMD ["python", "/home/jovyan/work/src/producer.py"]