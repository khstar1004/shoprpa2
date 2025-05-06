import os
import logging
import configparser
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.utils import formatdate
import datetime
from pathlib import Path

def send_excel_by_email(excel_paths, config, subject_prefix="ShopRPA 결과"):
    """
    Send Excel files as email attachments.
    
    Args:
        excel_paths (dict): Dictionary with keys 'result' and 'upload' containing paths to Excel files
        config (configparser.ConfigParser): Configuration object
        subject_prefix (str): Prefix for email subject
        
    Returns:
        bool: True if email was sent successfully, False otherwise
    """
    try:
        # Get email configuration from config
        smtp_server = config.get('Email', 'smtp_server', fallback='')
        smtp_port = config.getint('Email', 'smtp_port', fallback=587)
        smtp_username = config.get('Email', 'smtp_username', fallback='')
        smtp_password = config.get('Email', 'smtp_password', fallback='')
        sender_email = config.get('Email', 'sender_email', fallback='')
        recipient_emails = config.get('Email', 'recipient_emails', fallback='').split(',')
        use_tls = config.getboolean('Email', 'use_tls', fallback=True)
        
        # Check if required configuration is available
        if not (smtp_server and smtp_username and smtp_password and sender_email and recipient_emails):
            logging.error("Email configuration incomplete. Check config.ini [Email] section.")
            return False
            
        # Strip whitespace from recipient emails
        recipient_emails = [email.strip() for email in recipient_emails if email.strip()]
        if not recipient_emails:
            logging.error("No valid recipient emails found in configuration.")
            return False
            
        # Create message
        msg = MIMEMultipart()
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
        msg['From'] = sender_email
        msg['To'] = ", ".join(recipient_emails)
        msg['Date'] = formatdate(localtime=True)
        msg['Subject'] = f"{subject_prefix} - {timestamp}"
        
        # Email body
        body = f"""ShopRPA 작업이 완료되었습니다.

처리 시간: {timestamp}
첨부 파일: {len([p for p in excel_paths.values() if p and os.path.exists(p)])}개 첨부됨

이 메일은 자동으로 생성되었습니다.
"""
        msg.attach(MIMEText(body, 'plain'))
        
        # Attach Excel files
        attachments_added = 0
        for file_type, file_path in excel_paths.items():
            if not file_path or not os.path.exists(file_path):
                logging.warning(f"Excel file {file_type} does not exist at path: {file_path}")
                continue
                
            try:
                with open(file_path, 'rb') as file:
                    part = MIMEApplication(file.read(), Name=os.path.basename(file_path))
                    part['Content-Disposition'] = f'attachment; filename="{os.path.basename(file_path)}"'
                    msg.attach(part)
                    attachments_added += 1
                    logging.info(f"Attached file: {os.path.basename(file_path)}")
            except Exception as e:
                logging.error(f"Failed to attach {file_type} file: {e}")
        
        if attachments_added == 0:
            logging.warning("No attachments were added to the email.")
            # Decide whether to still send the email without attachments
            if not config.getboolean('Email', 'send_email_without_attachments', fallback=False):
                logging.info("Email not sent because there are no attachments.")
                return False
        
        # Send email
        with smtplib.SMTP(smtp_server, smtp_port) as server:
            if use_tls:
                server.starttls()
            server.login(smtp_username, smtp_password)
            server.send_message(msg)
            
        logging.info(f"Email sent successfully to {len(recipient_emails)} recipients with {attachments_added} attachments.")
        return True
        
    except Exception as e:
        logging.error(f"Failed to send email: {e}", exc_info=True)
        return False

def validate_email_config(config):
    """
    Validate email configuration in config.ini
    
    Args:
        config (configparser.ConfigParser): Configuration object
        
    Returns:
        bool: True if configuration is valid, False otherwise
    """
    try:
        # Check if Email section exists
        if 'Email' not in config:
            logging.warning("Email section missing from config.ini. Email functionality will be disabled.")
            return False
            
        # Check required fields
        required_fields = ['smtp_server', 'smtp_port', 'smtp_username', 
                          'smtp_password', 'sender_email', 'recipient_emails']
        
        missing_fields = [field for field in required_fields 
                         if not config.get('Email', field, fallback='')]
        
        if missing_fields:
            logging.warning(f"Missing email configuration fields: {', '.join(missing_fields)}. "
                           "Email functionality will be disabled.")
            return False
            
        # Validate SMTP port
        try:
            smtp_port = config.getint('Email', 'smtp_port', fallback=0)
            if smtp_port <= 0 or smtp_port > 65535:
                logging.warning(f"Invalid SMTP port: {smtp_port}. Must be between 1-65535.")
                return False
        except ValueError:
            logging.warning("SMTP port must be a valid integer.")
            return False
            
        # Validate at least one recipient email
        recipient_emails = config.get('Email', 'recipient_emails', fallback='').split(',')
        recipient_emails = [email.strip() for email in recipient_emails if email.strip()]
        if not recipient_emails:
            logging.warning("No valid recipient emails specified.")
            return False
            
        return True
        
    except Exception as e:
        logging.error(f"Error validating email configuration: {e}")
        return False 