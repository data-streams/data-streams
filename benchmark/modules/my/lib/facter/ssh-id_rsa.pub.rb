Facter.add('~/.ssh/id_rsa.pub') do
  setcode do
    Facter::Util::Resolution.exec('cat ~/.ssh/id_rsa.pub')
  end
end
