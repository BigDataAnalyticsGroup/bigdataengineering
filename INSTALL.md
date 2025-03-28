# Manual Installation

Throughout this lecture, we will make use of [Jupyter notebooks](https://jupyter.org/) to explain and illustrate underlying concepts. Some of these notebooks are also required for the assignment sheets.

In order to execute the notebooks, we basically only need a [`Python`](https://www.python.org/) installation and a couple of Python packages. In addition, a few of the notebooks require third-party software like [`PostgreSQL`](https://www.postgresql.org/) or [`Neo4j`](https://neo4j.com/). Therefore, we provide you with a [virtual machine](https://github.com/BigDataAnalyticsGroup/docker-bde) that already has everything installed.

Unfortunately, some of you encounter problems regarding the installation and/or use of the virtual machine.

**However, you do not necessarily need the virtual machine! For the assignment sheets, you will only need Python and the corresponding Python packages (no third-party software).** Thus, if you are unable to use the virtual machine for whatever reason, you can also manually install the required software. In the following, we will briefly show you how this can be done (the shell commands assume `macOS` as the operating system).

1. Install Python3 for your operating system. On `macOS`, we recommend the use of [Homebrew](https://brew.sh/index_de). The virtual machine uses `Python 3.10.12`. To be on the safe side, we also recommend the use of `Python 3.10.` Apparently, `Python 3.11` has a problem with the installation of at least one required package:
    ```sh
    $ brew install python@3.10
    ```
   
2. The [bigdataengineering](https://github.com/BigDataAnalyticsGroup/bigdataengineering) repository contains the notebook required for the lecture. Clone the repository to a folder of your choice on your machine:
    ```sh
    $ git clone --recursive https://github.com/BigDataAnalyticsGroup/bigdataengineering.git
    ```

3. Afterward, go into the `bigdataengineering` folder:
    ```sh
    $ cd bigdataengineering
   ```
   
4. Create a virtual environment. A guide for the creation of virtual environment (on different platforms) can be found [here](https://docs.python.org/3/library/venv.html?highlight=venv):
   ```sh
   $ python3.10 -m venv .
   ```

5. Once the virtual environment is created, it can be activated using the following commmand:
   ```sh
   $ source bin/activate
   ```
   Your terminal should now indicate that the virtual environment is activated, i.e., by the prefix `(notebooks)`.

6. Upgrade the package installer [pip](https://pypi.org/project/pip/) for Python and install the Python packages:
    ```sh
    (notebooks) $ pip install --upgrade pip
    (notebooks) $ pip install -r requirements.txt
    ```
    Now you should have all the Python packages installed and be ready to use the Jupyter notebooks.

7. Start the Jupyter server. Again, always make sure to activate the virtual environment before using Jupyter, otherwise it does not work:
    ```sh
    (notebooks) $ jupyter notebook
    ```
    This should automatically open Jupyter in your browser. If not, copy the shown URL and paste it in your browser.

8. After you have finished working on the notebooks, you can stop the Jupyter server by pressing `Ctrl-C` in your terminal and confirming with `y` and `Enter` (or by pressing `Ctrl-C` two times).  Afterwards, the virtual environment can be deactivated as follows:
   ```sh
   (notebooks) $ deactivate
   ```
   
Note that after you have successfully installed all required Python packages for the first time, your workflow to work on the notebooks only contains steps 3, 5, 7, and 8, i.e., you navigate to the `bigdataengineering` folder, activate the virtual environment, start the Jupyter server, work on the notebooks, stop the Jupyter server, and deactivate the virtual environment again.
