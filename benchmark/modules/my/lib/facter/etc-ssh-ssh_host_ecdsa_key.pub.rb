Facter.add('/etc/ssh/ssh_host_ecdsa_key.pub') do
  setcode do
    file = '/etc/ssh/ssh_host_ecdsa_key.pub'
    Facter::Util::Resolution.exec("[ -r #{file} ] && cat #{file}")
  end
end
