# Use the official Jupyter Notebook image with Python 3.9
FROM jupyter/scipy-notebook:python-3.9

# Install additional libraries
RUN pip install --no-cache-dir \
    pandas \
    numpy \
    matplotlib \
    seaborn \
    scikit-learn \
    xgboost \
    lightgbm \
    nltk \
    opencv-python \
    sympy \
    boto3 \
    requests \
    gql[requests] \
    json5 \
    jupyterlab

# Expose Jupyter port
EXPOSE 8888

# Start Jupyter Notebook with no token
CMD ["start-notebook.sh", "--NotebookApp.token=''"]


