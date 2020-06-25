#!/usr/bin/perl

#####################################################
## ������ ��� windows
## ������� ���������� ���� "out.zip" � �������:
## MESSAGE LOG
## ����� �������� �� ����� ���������� C:\Zip\out.zip
#####################################################
use strict;
use warnings;
#use locale;
#use utf8;
use DBI;
use IO::File;
use Archive::Zip;

my $path    = "C:\\Zip";  # ������� ��� ����������� "out.zip"
my $filezip = "out.zip";
my $maillog = "out";

my $hash;       # -- ������ �� Hash ������������ ��� �������������
my $message;    # -- ������ �� Hash ��� �������� � MESSAGE
my $log;        # -- ������ �� Hash ��� �������� � LOG
my $datetime;   # --
my $code;       # -- int_id
my $id;         # -- id
my $flag;       # -- ���� = (<=|=>|->|**|==)
my $string;     # -- ��������� ����� ������
my $dbh;        # -- dbi ���������� � ��
my $addr;       # -- ����� ���������� (�����������)
my $bool;       # -- ���������� ��������

my $zip = Archive::Zip->new("$path\\$filezip");  # ������������� ��� � ������� $path
   unless ($zip) {die "cannot open file";
}

my $member = $zip->memberNamed($maillog);
$zip->extractMember($member, "$path\\$maillog"); # ������������� ��� � ������� $path

my $fh = new IO::File;

if ($fh->open("< $path\\$maillog")) {
   while (<$fh>) {

     if ($_ =~ m/(\d{4})-(\d{2})-(\d{2})\s(\d{2}):(\d{2}):(\d{2})\s(\w{6})-(\w{6})-(\w{2})\s(<=|=>|->|\*\*|==)\s(.*?)$/) {
         #  DDDD-DD-DD DD:DD:DD XXXXXX-XXXXXX-XX (<=|=>|->|**|==) STRING
         $datetime = "$1-$2-$3 $4:$5:$6";
         $code     = "$7-$8-$9";
         $id       =  substr($7,-1).substr($8,-1).substr($9,0,2); # ���� ID - ���������� ;)
         $flag     = "$10";
         $string   = "$11";

         if ($flag eq '<=') { # �������� ��������� ���� = '<=' - ��������� ����� �����������

            $bool   = 'TRUE';

            if ($string =~ m/^((\w+?)\@(.+?))\s/) { # �������� ����� ����������� 
               $addr = $1;
               $string = $'; #$string =~ s/^$addr\s//;         # �� STRING ������� ����� ����������� - �������� ��
            } elsif ($string =~ m/^<>\s/) {
               $addr = '';                     
            } else {
               print "!Error: unaccounted situation (flag='<=')\n"; 
               exit;
            }

            ## CREATE HASH MESSAGE
            $hash->{TM}     = $datetime; # �������� ��� ��� MESSAGE ������ ���������
            $hash->{ID}     = $id;
            $hash->{INT_ID} = $code;
            $hash->{ADDR}   = $addr;
            $hash->{STR}    = $string;
            $hash->{BOOL}   = $bool;
            push(@{$message},$hash); # ���������� ������ �� ��� MESSAGE
            undef($hash);

         } else { # ������� �������� � LOG ��� ��������� ������, ��� ����� ����� '<='

            if ($string =~ m/^((\w+?)\@(.+?))\s/) { # �������� ����� ����������� 
               $addr = $1;
               $string = $'; #$string =~ s/^$addr\s//;         # �� STRING ������� ����� ����������� - �������� ��
            } else {
               $addr = '';
            }

            ## CREATE HASH LOG
            $hash->{TM}     = $datetime; # �������� ��� ��� LOG ������ ���������
            $hash->{INT_ID} = $code;
            $hash->{ADDR}   = $addr;
            $hash->{STR}    = $string;
            push(@{$log},$hash); # ���������� ������ �� ��� LOG
            undef($hash);
         }

     } elsif ($_ =~ m/(\d{4})-(\d{2})-(\d{2})\s(\d{2}):(\d{2}):(\d{2})\s(\w{6})-(\w{6})-(\w{2})\s(.*?)$/) {
         #  DDDD-DD-DD DD:DD:DD XXXXXX-XXXXXX-XX STRING
         $datetime = "$1-$2-$3 $4:$5:$6";
         $code     = "$7-$8-$9";
         $string   = "$10";

         ## CREATE HASH LOG
         $hash->{TM}     = $datetime; # �������� ��� ��� LOG ������ ���������
         $hash->{INT_ID} = $code;
         $hash->{ADDR}   = '';
         $hash->{STR}    = $string;
         push(@{$log},$hash); # ���������� ������ �� ��� LOG
         undef($hash);

     } elsif ($_ =~ m/(\d{4})-(\d{2})-(\d{2})\s(\d{2}):(\d{2}):(\d{2})\s(.*?)$/) {
         #  DDDD-DD-DD DD:DD:DD STRING
         $datetime = "$1-$2-$3 $4:$5:$6";
         $string   = "$7";

         ## CREATE HASH LOG
         $hash->{TM}     = $datetime; # �������� ��� ��� LOG ������ ���������
         $hash->{INT_ID} = $code;
         $hash->{ADDR}   = '';
         $hash->{STR}    = $string;
         push(@{$log},$hash); # ���������� ������ �� ��� LOG
         undef($hash);

     } else {
         print "!Error: unaccounted situation (flag != '<=')\n"; 
         exit;
     }
  }
  $fh->close;
}

$dbh = DBI->connect("dbi:Pg:dbname=postgres; host = 127.0.0.1; port = 5432","postgres","system",{AutoCommit => 0})
       or die $DBI::errstr;

foreach my $i (@{$message}) {
#print "insert into message (created,id,int_id,str,status) values (\'$i->{TM}\',\'$i->{ID}\',\'$i->{INT_ID}\',\'$i->{STR}\',\'$i->{BOOL}\');\n";
  my $stmt = qq{
     insert into log (created,int_id,str,address) values (\'$i->{TM}\',\'$i->{INT_ID}\',\$\$ $i->{STR} \$\$,\'$i->{ADDR}\');
     insert into message (created,id,int_id,str,status) values (\'$i->{TM}\',\'$i->{ID}\',\'$i->{INT_ID}\',\$\$ $i->{STR} \$\$,\'$i->{BOOL}\');     
  };
  my $rv = $dbh->do($stmt) or die $DBI::errstr;
}
$dbh->commit();

#@{$log} = (@{$log1}, @{$log2}); # ���������� ���� LOG = LOG1 + LOG2;
#undef($log1);   # ����������� ������
#undef($log2);

foreach my $i (@{$log}) {
#print "insert into log (created,int_id,str,address) values (\'$i->{TM}\',\'$i->{INT_ID}\',\'$i->{STR}\',\'$i->{ADDR}\');\n";
  my $stmt = qq{insert into log (created,int_id,str,address) values (\'$i->{TM}\',\'$i->{INT_ID}\',\$\$ $i->{STR} \$\$,\'$i->{ADDR}\');};
  my $rv = $dbh->do($stmt) or die $DBI::errstr;
}
$dbh->commit();
$dbh->disconnect();



exit;
__END__


