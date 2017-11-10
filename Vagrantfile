Vagrant.configure("2") do |config|
  config.env.enable
  config.vm.box = "centos/7"
  config.vm.hostname = "python-box"

  
  config.vm.network "forwarded_port", guest: 8080, host: 8080
  config.vm.network "forwarded_port", guest: 8888, host: 8888
  config.vm.network "forwarded_port", guest: 58846, host: 58846
  config.vm.network "forwarded_port", guest: 58946, host: 58946,
  auto_correct: true

  config.vm.synced_folder ".", "/vagrant", type: "rsync",
    rsync__exclude: [".git/"]
  config.vm.network "public_network"

  config.vm.provision "shell", path: "./provisioning_scripts/setup.sh"

  config.vm.provider "virtualbox" do |vb|
  #   # Customize the amount of memory on the VM:
    vb.memory = ENV['RAM'] || "8174"
    vb.cpus = ENV['CPUS'] || 4
   end
end
