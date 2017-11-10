import smtplib
import os
import getpass
email_user = input("From Email Address (must be an infobate email): ")
pswd = getpass.getpass('Password:')
to = input("To Email Address (leave empty if you want to send it to yourself): ")

email_password = pswd
email_address = email_user

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
server = 'mail.infobate.com'

# Create message container - the correct MIME type is multipart/alternative.
msg = MIMEMultipart('alternative')
msg['From'] = email_address
if len(to) > 1:
    msg['To'] = to
    email_to = to
else:
    msg['To'] = email_address
    email_to = email_address



def send_email_file_loaded(subject, message):
  msg['Subject'] = subject
  print(message)
  # Create the body of the message (a plain-text and an HTML version).
  html = """\
  <!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
  <html xmlns="http://www.w3.org/1999/xhtml">
  <head>
    <meta http-equiv="Content-Type" content="text/html; charset=utf-8" />
    <title>DataFile Alert</title>
  </head>
  <body>
        {error_message}
  </body>
  </html>
  """.format(**{ 'error_message': message })
  # Record the MIME types of both parts - text/plain and text/html.
  # part1 = MIMEText(subject, 'plain')
  part2 = MIMEText(html, 'html')
  # Attach parts into message container.
  # According to RFC 2046, the last part of a multipart message, in this case
  # the HTML message, is best and preferred.
  # msg.attach(part1)
  msg.attach(part2)
  smtp = smtplib.SMTP(server)
  smtp.login(email_user, email_password)
  smtp.sendmail(email_address, email_to, msg.as_string())
  smtp.close()

