# Obtain your API Key

If you just want to read data from HDX, then an API key is not necessary and you can
ignore the 6 steps below. However, if you want to write data to HDX, then you need to
register on the website to obtain an API key. You can supply this key as an argument or
create an API key file. If you create an API key file, by default this is assumed to be
called **.hdxkey** and is located in the current user's home directory **\~**. Assuming
you are using a desktop browser, the API key is obtained by:

1. Browse to the [HDX website](https://data.humdata.org/)
2. Left click on LOG IN in the top right of the web page if not logged in and log in
3. Left click on your username in the top right of the web page and select PROFILE from
the drop down menu
4. Scroll down to the bottom of the profile page
5. Copy the API key which will be of the form: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
6. You can either:

    a.  Pass this key as a parameter or within a dictionary

    b.  Create a JSON or YAML file. The default path is
            **.hdx_configuration.yaml** in the current user's home
            directory. Then put in the YAML file:

            hdx_key: "HDX API KEY"