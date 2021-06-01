# Web scrapping using Python

As part of my training at the [IA School](https://www.intelligence-artificielle-school.com/). I have to provide a tool that allows
the scrapping of the Open Food Facts web site available at [Open Food Facts](https://fr.openfoodfacts.org)

This script will:

- Use Webdriver /beautifulsoup to scrap the needed information.
- Start many workers (processes) that work simultanously. To do so, I use the multiprocess package. I split the number of pages by the number of workers.
- Each worker will write into a json file once a new product is retrieved. Once the worker is complete, it will, then, store the products information as csv file too 

Note: This script needs a python version 3. Ensure to install the following packages (pip install package-name)