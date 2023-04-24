# Manual Installation

Throughout this lecture, we will make use of [Jupyter notebooks](https://jupyter.org/) to explain and illustrate underlying concepts. Some of these notebooks are also required for the assignment sheets.

In order to execute the notebooks, we basically only need a [`Python`](https://www.python.org/) installation and a couple of Python packages. In addition, a few of the notebooks require third-party software like [`PostgreSQL`](https://www.postgresql.org/) or [`Neo4j`](https://neo4j.com/) (maybe we do not even need this). Therefore, we provide you with a [virtual machine](https://github.com/BigDataAnalyticsGroup/vagrant-bde) that already has everything installed.

Unfortunately, some of you encounter problems regarding the installation and/or use of the virtual machine, e.g. the installation does not work on MacBooks with M1 chips.


**However, you do not necessarily need the virtual machine! For the assignment sheets, you will only need Python and the corresponding Python packages (no third-party software).** Thus, if you are unable to use the virtual machine for whatever reason, you can also manually install the required software. In the following, we will briefly show you how this can be done (the shell commands assume `macOS` as the operating system).

1. Install Python3 for your operating system. On `macOS`, we recommend the use of [Homebrew](https://brew.sh/index_de). The virtual machine uses `Python 3.10.10`. To be on the safe side, we also recommend the use of `Python 3.10.` Apparently, `Python 3.11.` has a problem with the installation of at least one required package.

    ```sh
    $ brew install python@3.10
    ```
2. Create a virtual environment. A guide for the creation of virtual environment (on different platforms) can be found
   [here](https://docs.python.org/3/library/venv.html?highlight=venv). Make sure to use the correct Python version in
   the following!
   ```sh
   $ python -m venv /path/to/new/virtual/environment
   ```
   Once the virtual environment is created, it can be activated using the following commmand, with `<venv>` being the
   path to the virtual environment from before.
   ```sh
   $ source <venv>/bin/activate
   ```
   Your terminal should now indicate that the virtual environment is activated. It can be deactivated by typing
   ```sh
   $ deactivate
   ```

3. The [bigdataengineering](https://github.com/BigDataAnalyticsGroup/bigdataengineering) repository contains the
   notebook required for the lecture. Clone the repository to a folder of your choice on your machine.
    ```sh
    $ git clone --recursive https://github.com/BigDataAnalyticsGroup/bigdataengineering.git
    ```
4. Afterwards, go into the `bigdataengineering` folder and make sure that the virtual environment is active.
    ```sh
    $ cd bigdataengineering
    $ source <venv>/bin/activate
    ```
5. Upgrade the package installer [`pip`](https://pypi.org/project/pip/) for Python and install the Python packages.
    ```sh
    $ pip install --upgrade pip
    $ pip install -r requirements.txt
    ```
    Now you should have all the Python packages installed and be ready to use the Jupyter notebooks.

6. Start the Jupyter server. Again, always make sure to activate the virtual environment before using Jupyter, otherwise
   it does not work.
    ```sh
    $ jupyter notebook
    ```
    This should automatically open Jupyter in your browser.
