# parquet dependencies
* window10+python36
    * pip install fastparquet
    * pip install ./resources/python_snappy-0.5.3-cp36-cp36m-win_amd64.whl
        * download *.whl from 'https://www.lfd.uci.edu/~gohlke/pythonlibs/' website. 


* CentOS+python36
    * pip install fastparquet
    * pip install python-snappy
    <br>`Note:if install failed, maybe you need to excute 'yum install snappy-devel' before install python-snappy`
        
# avro dependencies
* window10+python36
    * pip install avro-python3==1.8.2
    <br>`link : https://pypi.org/project/avro-python3/`.      