# Use Facades

The simple facade makes it easier to get up and running:

    from hdx.facades.simple import facade

    def main():
        ***YOUR CODE HERE***

    if __name__ == "__main__":
        facade(main, CONFIGURATION_KWARGS)

The keyword arguments facade is similar but passes through keyword arguments:

    from hdx.facades.keyword_arguments import facade

    def main(kwparam1, kwparam2, ...,**ignore):
        ***YOUR CODE HERE***

    if __name__ == "__main__":
        facade(main, CONFIGURATION_AND_OTHER_KWARGS)

The infer arguments facade infers the possible command line from the type hints and
docstring describing the parameters of the function you give (such as main). It is
possible to pass additional parameters to the facade which will be overridden by any
parameters provided on the command line with the same name.

    from hdx.facades.infer_arguments import facade

    def main(kwparam1: bool, kwparam2: str):
        """Generate dataset and create it in HDX
        Args:
            kwparam1 (bool): Help text for this command line argument
            kwparam2 (str): Help text for this command line argument

        Returns:
            None
        """
        ***YOUR CODE HERE***

    if __name__ == "__main__":
        facade(main, kwparam3="lala")
