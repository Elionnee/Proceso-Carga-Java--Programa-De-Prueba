 CREATE DATABASE database1;
 CREATE USER 'root' identified by 'root';
 GRANT ALL PRIVILEGES ON *.* TO 'root'@'%';
 FLUSH PRIVILEGES;