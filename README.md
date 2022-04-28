# Big Data Engineering

This repository contains code used during my lectures  _Big Data Engineering_.

Notice. This educational software.
We do not give any warranty or guarantee for fitness for a particular purpose, correctness, performance, etc.


# Foreword

1. We spent a lot of time preparing the tools and the write ups below. Please read them carefully before asking questions!
2. If you are not yet familiar with terminal usage, you should spend some time to learn the basics.
3. If your terminal spits out a warning or an error, don't panic! Read the error message, it often hints at possible solutions. It is highly likely that someone else experienced the exact same problem so use the forums to get in touch with other students and tutors.


# Vagrant

Throughout this lecture, we will make use of Jupyter notebooks. In order to execute these notebooks, we provide you with a virtual machine. This virtual machine is based on [VirtualBox](https://www.virtualbox.org/), [Vagrant](https://www.vagrantup.com/) acts as a middleman that lets us create and configure with the virtual machine by means of a so called `Vagrantfile`. A `Vagrantfile` is basically a script that tells vagrant what commands to execute when first booting up the virtual machine. Whenever you interact with the virtual machine, you will do so through vagrant!

## Preliminary

First, you have to download and install both [VirtualBox](https://www.virtualbox.org/wiki/Downloads) and [Vagrant](https://www.vagrantup.com/downloads.html) for your operating system either through the downloads on the respective website or your systems package manager. Note that on macOS, you have to allow a kernel extension to be installed by VirtualBox under Security in your Settings.app. With the following commands, you can test if the installation was successful. Note that version numbers may slightly differ:
```sh
$ VBoxManage -v
6.1.4r136177
$ vagrant -v
Vagrant 2.2.7
```
Afterwards, download the `Vagrantfile` from the [GitHub page](https://github.com/BigDataAnalyticsGroup/python) and place it in an empty directory of your choice. Navigate to the directory and make sure that it only contains the `Vagrantfile`. In some cases, the `Vagrantfile` is assigned a suffix during download. In this case, the `Vagrantfile` has to be renamed to `Vagrantfile`. If you now list the contends of the directory you are currently in, you should receive the following output:
```sh
$ ls 
Vagrantfile
```
The `Vagrantfile` defines a virtual machine with [Arch Linux](https://www.archlinux.org/) as the operating system and contains several scripts for installing and configuring the required software on the virtual machine.

## Basic Usage

Next, we explain the main functionality and usage of Vagrant. To start the virtual machine, execute the following command in the vagrant directory:
```sh
$ vagrant up
Bringing machine 'bde_box' up with 'virtualbox' provider...
==> bde_box: Checking if box 'archlinux/archlinux' version '2020.03.04' is up to date...
==> bde_box: Clearing any previously set forwarded ports...
==> bde_box: Clearing any previously set network interfaces...
==> bde_box: Preparing network interfaces based on configuration...
    bde_box: Adapter 1: nat
==> bde_box: Forwarding ports...
    bde_box: 8888 (guest) => 8888 (host) (adapter 1)
    bde_box: 22 (guest) => 2222 (host) (adapter 1)
==> bde_box: Running 'pre-boot' VM customizations...
==> bde_box: Booting VM...
==> bde_box: Waiting for machine to boot. This may take a few minutes...
    bde_box: SSH address: 127.0.0.1:2222
    bde_box: SSH username: vagrant
    bde_box: SSH auth method: private key
==> bde_box: Machine booted and ready!
==> bde_box: Checking for guest additions in VM...
==> bde_box: Setting hostname...
==> bde_box: Mounting shared folders...
    bde_box: /home/vagrant/shared => /Users/student/Repos/vagrant/shared
==> bde_box: Machine already provisioned. Run `vagrant provision` or use the `--provision`
==> bde_box: flag to force provisioning. Provisioners marked to run always will still run.
```
When executed for the first time, this command creates the virtual machine and executes all configuration scripts. This process might take a while. Make sure to have a stable internet connection! (University Wifi is not a stable internet connection.) Don't panic if you see some red output on your terminal, this is perfectly fine.
Afterwards, the virtual machine is running on your machine and you can connect to it via ssh. For this, use the following vagrant command:
```sh
$ vagrant ssh
(bde) [vagrant@archlinux ~]$
```
You have now successfully logged into your virtual machine. Your username and password are *vagrant*. This user has superuser privileges. 
To close the ssh connection you can use the `exit` command:
```sh
(bde) [vagrant@archlinux ~]$ exit
logout
Connection to 127.0.0.1 closed.
```
The virtual machine is still running in the background. To check the current status of your virtual machine, you can use the `status` command:
```sh
$ vagrant status
Current machine states:

bde_box                   running (virtualbox)

The VM is running. To stop this VM, you can run `vagrant halt` to
shut it down forcefully, or you can run `vagrant suspend` to simply
suspend the virtual machine. In either case, to restart it again,
simply run `vagrant up`.
```
To shutdown the virtual machine use the command `halt`:
```sh
$ vagrant halt
==> bde_box: Attempting graceful shutdown of VM...
```

## Shared Folder

The virtual machine sets up a shared folder called shared. This folder is synchronized between the host (your local machine) and the guest (virtual machine). It allows to easily move files between the two systems. On the virtual machine, the folder is located in the home directory `/home/vagrant/shared`. On your local machine, the folder is located in the same directory as your `Vagrantfile`.

## Cheat Sheet

**Starting and stopping virtual machine**
* `vagrant up`: starts virtual machine, runs provision (setup, configuration) on first call
* `vagrant halt`: stops the virtual machine
* `vagrant resume`: resumes a suspended virtual machine
* `vagrant suspend`: suspends the virtual machine (remembers state)
* `vagrant reload`: restarts the virtual machine, basically vagrant halt and vagrant up (also loads new Vagrantfile configuration)

**Connecting to a virtual machine**
* `vagrant ssh`: connect to the virtual machine via SSH
* `vagrant ssh <boxname>`: connect to a virtual machine with a specific name, useful if multiple machines are running

**Other commands**
* `vagrant`: display a list of all available commands
* `vagrant -v`: display the version of vagrant
* `vagrant status`: outputs status of the vagrant machine
* `vagrant provision`: forces re-provisioning (installation and configuration scripts) of the vagrant machine
* `vagrant destroy`: stops and deletes all traces of the vagrant machine

For more details, please visit the [official Vagrant documentation](https://www.vagrantup.com/docs/).


# Workflow

In this section, we will discuss the usual workflow when using vagrant in the context of this lecture. We assume that the virtual machine is already created and currently running.

## Clone the Repository

On the host, clone this repository to the shared folder. Make sure, that submodules are also loaded by using the `--recursive` option.
```sh
$ cd shared
$ git clone --recursive https://github.com/BigDataAnalyticsGroup/bigdataengineering
```
Now your virtual machine has access to the notebooks and is ready to execute them using Jupyter.

## Jupyter

The Jupyter notebooks are executed inside the virtual machine but can be displayed it in the browser of the local machine. (This is achieved by forwarding the port 8888 of yhe virtual machine to your local machine). First, navigate to the directory containing the notebook you would like to execute on the virtual machine, e.g. as follows:
```sh
(bde) [vagrant@archlinux ~]$ cd shared/bigdataengineering
```
Then start the Jupyter notebook server on the virtual machine with the following command. Note that port forwarding only works if you provide the argument `--ip=0.0.0.0`.
```sh
(bde) [vagrant@archlinux bigdataengineering]$ jupyter notebook --no-browser --ip=0.0.0.0
[I 14:14:20.847 NotebookApp] [jupyter_nbextensions_configurator] enabled 0.4.1
[I 14:14:20.847 NotebookApp] Serving notebooks from local directory: /home/vagrant
[I 14:14:20.848 NotebookApp] The Jupyter Notebook is running at:
[I 14:14:20.848 NotebookApp] http://(archlinux or 127.0.0.1):8888/?token=31c1124227df5f9921b663cbcef88abf4352b6ae2785c847
[I 14:14:20.848 NotebookApp] Use Control-C to stop this server and shut down all kernels (twice to skip confirmation).
[C 14:14:20.850 NotebookApp]

    To access the notebook, open this file in a browser:
        file:///home/vagrant/.local/share/jupyter/runtime/nbserver-40139-open.html
    Or copy and paste one of these URLs:
        http://(archlinux or 127.0.0.1):8888/?token=31c1124227df5f9921b663cbcef88abf4352b6ae2785c847
...
```
To access the Jupyter server from your local browser, copy the link at the bottom from the terminal, paste it into the address bar of your browser, replace the part in bracktes by `localhost`, and hit enter. In this example, the link would be `http://localhost:8888/?token=31c1124227df5f9921b663cbcef88abf4352b6ae2785c847`. The Jupyter server opens in your browser and you see a similar page as below.
![Jupyter Notebook](https://i.imgur.com/0egNn9r.jpg)
You can now execute the notebooks from the lecture.
To stop the Jupyter server, you can press `Ctrl-C` in your terminal and afterwards, confirm with `y` and enter.
```sh
...
[[I 10:43:52.825 NotebookApp] interrupted
Serving notebooks from local directory: /home/vagrant/notebooks
0 active kernels
The Jupyter Notebook is running at:
http://archlinux:8888/?token=6585ef8be9a9c58f17953e725450909d62051515e0b0da1a
 or http://127.0.0.1:8888/?token=6585ef8be9a9c58f17953e725450909d62051515e0b0da1a
Shutdown this notebook server (y/[n])? y
[C 10:43:53.906 NotebookApp] Shutdown confirmed
[I 10:43:53.907 NotebookApp] Shutting down 0 kernels
```
