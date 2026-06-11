SELECT
    session_id, label,
    CASE
        WHEN label = 'normal' THEN 'normal'
        WHEN label IN ('neptune','back','land','pod','smurf','teardrop',
                       'apache2','mailbomb','processtable','udpstorm') THEN 'dos'
        WHEN label IN ('ipsweep','nmap','portsweep','satan',
                       'mscan','saint') THEN 'probe'
        WHEN label IN ('ftp_write','guess_passwd','imap','multihop','phf',
                       'spy','warezclient','warezmaster','sendmail','named',
                       'snmpgetattack','snmpguess','xlock','xsnoop','worm') THEN 'r2l'
        WHEN label IN ('buffer_overflow','loadmodule','perl','rootkit',
                       'httptunnel','ps','sqlattack','xterm') THEN 'u2r'
        ELSE 'unknown'
    END AS attack_category,
    CASE WHEN label = 'normal' THEN 0 ELSE 1 END AS is_attack,
    duration, protocol_type, service, flag,
    src_bytes, dst_bytes, land, wrong_fragment, urgent,
    hot, num_failed_logins, logged_in, num_compromised,
    root_shell, su_attempted, num_root, num_file_creations,
    num_shells, num_access_files, num_outbound_cmds,
    is_host_login, is_guest_login,
    count, srv_count, serror_rate, srv_serror_rate,
    rerror_rate, srv_rerror_rate, same_srv_rate, diff_srv_rate,
    srv_diff_host_rate, dst_host_count, dst_host_srv_count,
    dst_host_same_srv_rate, dst_host_diff_srv_rate,
    dst_host_same_src_port_rate, dst_host_srv_diff_host_rate,
    dst_host_serror_rate, dst_host_srv_serror_rate,
    dst_host_rerror_rate, dst_host_srv_rerror_rate
FROM {{ ref('stgrawlogs') }}
