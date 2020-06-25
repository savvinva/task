#!C:\Perl64\bin\perl.exe 
use strict;
use warnings;

use CGI::Carp qw(fatalsToBrowser); # send errors to the browser, not to the logfile
use CGI;
use DBI;

my $cgi = new CGI; #CGI->new;

print $cgi->header;
print $cgi->start_html();


my $dbh = DBI->connect("dbi:Pg:dbname=postgres; host = 127.0.0.1; port = 5432","postgres","system",{AutoCommit => 0})
       or die $DBI::errstr;

my $addr = $cgi->param('address');

#print "addr=$addr";

my $query = "SELECT l.created cre,l.str str
             FROM message m  
             RIGHT JOIN log l ON m.created = l.created and m.int_id = l.int_id
             where l.address = \'$addr\'
             order by m.int_id, m.created";

my $sth = $dbh->prepare($query);
my $rv = $sth->execute(); # Возвращает количество записей

my $i = 0; # Для подсчета количества строк

print '<table width="100%" cellspacing="0" cellpadding="0">';
while (my $ref = $sth->fetchrow_hashref()) {
   $i++;
   print "<tr><td>$ref->{cre}</td> <td>$ref->{str}</td></tr>\n";
   if ($i > 100) {
      print "</table>";
      print "<h3>Your query returns more than 100 rows!</h3>";
      last;
   }
}
print "</table>";


$sth->finish();
$dbh->disconnect();

print $cgi->end_html;




