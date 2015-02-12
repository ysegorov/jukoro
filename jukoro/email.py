# -*- coding: utf-8 -*-

from __future__ import absolute_import

import os
import smtplib

from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders


def send_mail(send_to, subject, text, smtp_from='noreply@example.com',
              smtp_host='localhost', smtp_port=smtplib.SMTP_PORT,
              smtp_login=None, smtp_pass=None, use_tls=False, files=None):
    """
    Mailer
    source: http://stackoverflow.com/a/3363254

    """
    files = [] if files is None else files

    assert isinstance(send_to, (list, tuple))
    assert isinstance(files, (list, tuple))

    msg = MIMEMultipart('alternative')
    msg.set_charset('utf-8')

    msg['From'] = smtp_from
    msg['To'] = COMMASPACE.join(send_to)
    msg['Date'] = formatdate(localtime=True)
    msg['Subject'] = subject

    msg.attach(MIMEText(text, 'plain'))
    msg.attach(MIMEText(text, 'html'))

    for f in files:
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(open(f, 'rb').read())
        Encoders.encode_base64(part)
        part.add_header('Content-Disposition',
                        'attachment; filename="%s"' % os.path.basename(f))
        msg.attach(part)

    smtp = smtplib.SMTP(host=smtp_host, port=smtp_port)

    if use_tls:
        smtp.ehlo()
        smtp.starttls()
        smtp.ehlo()

    if smtp_login and smtp_pass:
        smtp.login(smtp_login, smtp_pass)

    smtp.sendmail(smtp_from, send_to, msg.as_string())
    smtp.close()
