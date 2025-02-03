ZKteco clocking device integration for gathering clocking Data from each device as object and posting the data to the API endpoint specified
 NB! 
 Before utilising this script it is important to firstly run the following commands under Sudo user permissions in order for it to work effectively when using unix-based systems such as AWS etc.
 *yum update
 *yum upgrade
 *yum install git -y
 *yum update python3
 *yum install python3-pip
 *pip install pipreqs \directory of repository files
this will ensure that the prerequisits for running the script is met.
to set the parameters such as the device's ip address and port utilise the settings.json file accordingly.
a Screen -S command can be used to keep the script alive
use git clone "git directory" to install script files to local machine
use "git clone --depth=1 https://github.com/JacquesStrydom94/ZKTeco-Integration-Script.git ."
pip3 uninstall urllib3
pip3 install urllib3==1.26.14
pip3 uninstall requests
pip3 install requests
