Facter.add('/etc/ssh/ssh_host_ecdsa_key.pub') do
  setcode do
    Facter::Util::Resolution.exec('cat /etc/ssh/ssh_host_ecdsa_key.pub')
  end
end
