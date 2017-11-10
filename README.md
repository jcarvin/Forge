# How to get this thing started:
---
### Step one: 

* Clone the repo:
    * `git clone git@gitlab.infobate.com:jcarvin/geo_tagging.git`
* Boot the vagrant box 
    * `vagrant up`
    * `vagrant ssh`
* And finally, do this:
    * `sudo docker run --privileged=true -it -p 8888:8888 -v /vagrant/:/home/scientist/host --name pythyme jcarvin/pythyme`
    
    ---
    ### Step two:
    
* Now you're in the docker container inside the vagrant box on your host machine. You can do all kinds of things.
    * `jupyter notebook --ip=0.0.0.0`
        * Note: you may need to run this as sudo. And if sudo doesn't work, try `psudo() { sudo env PATH="$PATH" "$@"; }` and run `psudo jupyter notebook --ip=0.0.0.0`
    * `python /home/scientist/input/app.py`
        * Note: this may also need run as sudo (or psudo)