# Brief History

The first iteration of a scraper for ACLED was written without the HDX Python library
and it became clear looking at this and previous work by others that there are operations
that are frequently required and which add unnecessary complexity to the task of coding
against HDX. Simplifying the interface to HDX drove the development of the Python
library and the second iteration of the scraper was built using it. ACLED went from
producing files to creating an API, so a third iteration was developed.

With the interface using HDX terminology and mapping directly on to datasets, resources
and showcases, the ACLED scraper was faster to develop and was much easier to understand
for someone inexperienced in how it works and what it is doing. The extensive logging
and transparent communication of errors is invaluable and enables action to be taken to
resolve issues as quickly as possible. Static metadata can be held in human readable
files so if it needs to be modified, it is straightforward.

The HDX Python library has expanded over time as needs have arisen and is used for a
range of tasks involving interaction with the HDX platform.