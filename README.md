ECMWF Python Client
===================

This repository is a copy of the ECMWF Python Client 1.4.1 obtained from the [ECMWF webpages](https://software.ecmwf.int/wiki/display/WEBAPI/Accessing+ECMWF+data+servers+in+batch), that is used to access ECMWF datasets. It is part of my MSc dissertation project at the University of Edinburgh.

The project improved the robustness of transfers and added several features, including parallel downloading and a version of the client that operates in the background of a system. 

Usage
-----
This client requires the `httplib2` to be installed. You can install it with `pip`: ```pip3 install httplib2``` or ```pip3 install --user httplib2``` to install for the local user only.

The file `example.py` contains an example of a normal dataset request, without parallelisation. It can be called using ```python3 example.py``` A parallel request, that transfers multiple datasets concurrently, can be started with ```python3 example_parallel.py```

Standard requests are configured for both examples. More example requests can be found on the [ECMWF webpages](https://software.ecmwf.int/wiki/display/WEBAPI/Accessing+ECMWF+data+servers+in+batch).

This client contains a background-client that runs in the background of a system. It can be started with `python3 background_client_cli.py start`. Further usage instructions can be obtained through `python3 background_client_cli.py help`. 

To access the API, users need to obtain MARS API credentials on the [ECMWF webpages](https://apps.ecmwf.int/registration/). These details can either be stored in the file ```~/.ecmwfapirc``` as the MARS webpages suggest, or entered in the configuration file.

This project also provides a web interface that can be used to operate the background client: https://github.com/rpersoon/ecmwf-python-client-webinterface. 