#!/bin/ksh
# ==============================================================================
# Script Name : refresh-pipeline.sh
# Version     : 9.5
# Description : Interactive OGG Microservices Pipeline Manager
#               - Replicats & Extracts (List, Start, Stop, Drop, Recreate, Create)
#               - Distribution Paths (List, Start, Stop, Drop, Recreate, Create, Alter)
#               - DB Connections / Credential Store (List, Create/Update, Drop)
#               - PurgeExtTrails Tasks (19c CLI + Native AIX Perl REST Client)
# OS / Shell  : AIX 7 (ksh88 compliant) | OGG 23c/26ai Server / 19c AdminClient
#
# Optional External Configuration File (ogg_config.cfg):
#   OGG_URL="http://<OGG_HOSTNAME>:<SERVICE_MANAGER_PORT>"
#   OGG_USER="<OGG_ADMIN_USER>"
#   OGG_PASS="<OGG_ADMIN_PASSWORD>"
#   DEPLOYMENT="<OGG_DEPLOYMENT_NAME>"
#   TRAIL_NAME="./dirdat/tr"
# ==============================================================================

SCRIPT_VERSION="9.5"
ADMINCLIENT="${ADMINCLIENT:-/u01/app/ogg/bin/adminclient}"
CONFIG_FILE="ogg_config.cfg"

# Ensure AIX freeware paths are included if present
PATH=$PATH:/opt/freeware/bin:/usr/local/bin
export PATH

# ------------------------------------------------------------------------------
# 1. Load Configuration & Generic Fallback Defaults
# ------------------------------------------------------------------------------
if [ -f "${CONFIG_FILE}" ]; then
    . ./${CONFIG_FILE}
fi

OGG_URL="${OGG_URL:-http://<OGG_HOSTNAME>:<PORT>}"
OGG_USER="${OGG_USER:-<OGG_ADMIN_USER>}"
OGG_PASS="${OGG_PASS:-}"
TRAIL_NAME="${TRAIL_NAME:-./dirdat/tr}"

# ------------------------------------------------------------------------------
# 2. Setup Dynamic Working Directory & Generate Safe AWK / Perl Scripts
# ------------------------------------------------------------------------------
TIMESTAMP=`date +"%Y%m%d_%H%M%S"`
WORKDIR="/tmp/ogg_manager_${TIMESTAMP}"
PRM_BACKUP_DIR="${WORKDIR}/prm_backups"
PATH_BACKUP_DIR="${WORKDIR}/path_backups"

mkdir -p "${PRM_BACKUP_DIR}" "${PATH_BACKUP_DIR}"
if [ $? -ne 0 ]; then
    echo "CRITICAL ERROR: Failed to create dynamic working directory ${WORKDIR}."
    exit 1
fi

# Native AIX Perl HTTP/REST Client (No curl or extra packages required)
cat << 'EOF_PERL' > "${WORKDIR}/ogg_rest.pl"
use strict;
use warnings;
use IO::Socket::INET;
use MIME::Base64;

my ($method, $url, $user, $pass, $payload) = @ARGV;
$payload = "" unless defined $payload;

if ($url =~ m|^http://([^/:]+):(\d+)(/.*)$|) {
    my ($host, $port, $path) = ($1, $2, $3);
    my $auth = encode_base64("$user:$pass", "");
    my $sock = IO::Socket::INET->new(
        PeerAddr => $host,
        PeerPort => $port,
        Proto    => 'tcp',
        Timeout  => 15
    );
    if (!$sock) {
        print "REST ERROR: Cannot connect to $host:$port ($!)\n";
        exit 1;
    }
    my $len = length($payload);
    print $sock "$method $path HTTP/1.0\r\n";
    print $sock "Host: $host:$port\r\n";
    print $sock "Authorization: Basic $auth\r\n";
    print $sock "Accept: application/json\r\n";
    if ($len > 0) {
        print $sock "Content-Type: application/json\r\n";
        print $sock "Content-Length: $len\r\n";
    }
    print $sock "Connection: close\r\n\r\n";
    print $sock $payload if $len > 0;

    my $in_body = 0;
    my $body = "";
    while (my $line = <$sock>) {
        if (!$in_body && $line =~ /^\r?\n$/) {
            $in_body = 1;
            next;
        }
        $body .= $line if $in_body;
    }
    close($sock);

    # Simple pretty-print for JSON output on terminal
    $body =~ s/,"/",\n  "/g;
    $body =~ s/\{/\{\n  /g;
    $body =~ s/\}/\n\}/g;
    print "$body\n";
} else {
    print "REST Notice: Native AIX socket requires http://host:port format (Current: $url)\n";
}
EOF_PERL

cat << 'EOF_AWK' > "${WORKDIR}/extract_prm.awk"
/^OGG .* 3> / { sub(/^OGG [^>]*> /, ""); in_prm=1; next }
/^view params / || /^OGG .* 4> / { in_prm=0 }
in_prm { print }
EOF_AWK

cat << 'EOF_AWK' > "${WORKDIR}/get_alias.awk"
!/^[[:space:]]*--/ && toupper($0) ~ /USERIDALIAS/ {
    for (i=1; i<=NF; i++) { if (toupper($i) == "USERIDALIAS") { nxt = i + 1; val = $nxt; gsub(/[,;]/, "", val); print val; exit } }
}
EOF_AWK

cat << 'EOF_AWK' > "${WORKDIR}/get_domain.awk"
!/^[[:space:]]*--/ && toupper($0) ~ /USERIDALIAS/ {
    for (i=1; i<=NF; i++) { if (toupper($i) == "DOMAIN") { nxt = i + 1; val = $nxt; gsub(/[,;]/, "", val); print val; exit } }
}
EOF_AWK

cat << 'EOF_AWK' > "${WORKDIR}/get_chkpt.awk"
/^Checkpoint table/ { val = $3; gsub(/\\n.*|[\42,]/, "", val); print val; exit }
EOF_AWK

cat << 'EOF_AWK' > "${WORKDIR}/get_exttrail.awk"
!/^[[:space:]]*--/ && toupper($1) == "EXTTRAIL" { val = $2; gsub(/[,;]/, "", val); print val; exit }
EOF_AWK

cat << 'EOF_AWK' > "${WORKDIR}/get_pdb.awk"
!/^[[:space:]]*--/ && toupper($1) == "SOURCECATALOG" { val = $2; gsub(/[,;]/, "", val); print val; exit }
EOF_AWK

cat << 'EOF_AWK' > "${WORKDIR}/parse_deps.awk"
BEGIN { in_table=0 }
/^[[:space:]]*-[[:space:]]+[A-Za-z0-9_]/ { print $2; next }
/^Deployment Name:[[:space:]]*/ { print $3; next }
/^Deployment/ { in_table=1; next }
/^-/ && in_table { next }
/^$/ || /^OGG / || /^====/ { in_table=0 }
in_table && NF > 0 && !/^Available/ { print $1 }
EOF_AWK

cat << 'EOF_AWK' > "${WORKDIR}/get_rep_trail.awk"
/Log Read Checkpoint.*File/ {
    for (i=1; i<=NF; i++) {
        if ($i == "File") {
            val = $(i+1);
            gsub(/[0-9]+$/, "", val);
            print val;
            exit;
        }
    }
}
EOF_AWK

cat << 'EOF_AWK' > "${WORKDIR}/get_mode.awk"
BEGIN { p = 0; i = 0; c = 0 }
NR <= 30 {
    line = toupper($0)
    if (line ~ /PARALLEL/) p = 1
    if (line ~ /INTEGRATED/) i = 1
    if (line ~ /COORDINATED/) c = 1
}
END { 
    if (p && i) print "PARALLEL INT"
    else if (p) print "PARALLEL"
    else if (i) print "INTEGRATED"
    else if (c) print "COORDINATED"
    else print "CLASSIC"
}
EOF_AWK

# --- Distribution Path Parsers (19c/23c Compatible) ---
cat << 'EOF_AWK' > "${WORKDIR}/parse_path_list.awk"
/^[[:space:]]*Path Name[[:space:]]*:/ {
    sub(/^[[:space:]]*Path Name[[:space:]]*:[[:space:]]*/, "")
    print $1
    next
}
/^[[:space:]]*(RUNNING|STOPPED|ABENDED)[[:space:]]+/ {
    print $2
    next
}
/^[[:space:]]*[A-Za-z0-9_-]+[[:space:]]+(RUNNING|STOPPED|ABENDED)[[:space:]]+/ {
    if ($1 != "EXTRACT" && $1 != "REPLICAT" && $1 != "Status" && $1 != "Program" && $1 != "Path") {
        print $1
    }
}
EOF_AWK

cat << 'EOF_AWK' > "${WORKDIR}/get_path_src.awk"
{
    for (i=1; i<=NF; i++) {
        if ($i ~ /^trail:\/\//) {
            val = $i
            gsub(/[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]$/, "", val)
            print val
            exit
        }
    }
}
EOF_AWK

cat << 'EOF_AWK' > "${WORKDIR}/get_path_dst.awk"
{
    for (i=1; i<=NF; i++) {
        if ($i ~ /^(ws|wss|ogg|oggssl):\/\//) {
            val = $i
            gsub(/[0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9][0-9]$/, "", val)
            print val
            exit
        }
    }
}
EOF_AWK

cat << 'EOF_AWK' > "${WORKDIR}/get_path_auth.awk"
toupper($0) ~ /AUTHENTICATION METHOD/ || toupper($0) ~ /USERIDALIAS/ {
    line = $0
    sub(/^.*:[[:space:]]*/, "", line)
    if (toupper(line) ~ /USERIDALIAS/) {
        print line
        exit
    }
}
EOF_AWK

cat << 'EOF_AWK' > "${WORKDIR}/get_path_enc.awk"
toupper($0) ~ /ENCRYPTION PROFILE/ || toupper($0) ~ /ENCRYPTION ALGORITHM/ {
    line = $0
    sub(/^.*:[[:space:]]*/, "", line)
    if (toupper(line) !~ /NONE/ && line != "") {
        print line
        exit
    }
}
EOF_AWK

# ------------------------------------------------------------------------------
# 3. HELPER FUNCTIONS: Run OGG CLI & REST Commands safely
# ------------------------------------------------------------------------------
function run_ogg_sm_cmd {
    _LOG_FILE="$1"
    
    rm -f "${WORKDIR}/_exec.oby"
    echo "connect ${OGG_URL} as ${OGG_USER} password ${OGG_PASS}" > "${WORKDIR}/_exec.oby"
    echo "info deployment *" >> "${WORKDIR}/_exec.oby"
    echo "exit" >> "${WORKDIR}/_exec.oby"
    
    cat "${WORKDIR}/_exec.oby" | "${ADMINCLIENT}" > "${_LOG_FILE}" 2>&1
}

function run_ogg_cmd {
    _LOG_FILE="$1"
    shift
    
    rm -f "${WORKDIR}/_exec.oby"
    echo "connect ${OGG_URL} deployment ${DEPLOYMENT} as ${OGG_USER} password ${OGG_PASS}" >> "${WORKDIR}/_exec.oby"
    echo "set pager cat" >> "${WORKDIR}/_exec.oby"
    
    while [ $# -gt 0 ]; do
        echo "$1" >> "${WORKDIR}/_exec.oby"
        shift
    done
    
    echo "exit" >> "${WORKDIR}/_exec.oby"
    
    if [ "${_LOG_FILE}" = "STDOUT" ]; then
        cat "${WORKDIR}/_exec.oby" | "${ADMINCLIENT}"
    else
        cat "${WORKDIR}/_exec.oby" | "${ADMINCLIENT}" > "${_LOG_FILE}" 2>&1
    fi
}

# Native REST caller for AIX (uses curl if installed, otherwise native AIX Perl)
function run_ogg_rest {
    _METHOD="$1"
    _ENDPOINT="$2"
    _PAYLOAD="$3"
    _FULL_URL="${OGG_URL}${_ENDPOINT}"

    if [ -x "/opt/freeware/bin/curl" ]; then
        if [ -n "${_PAYLOAD}" ]; then
            /opt/freeware/bin/curl -s -k -u "${OGG_USER}:${OGG_PASS}" -X "${_METHOD}" -H "Content-Type: application/json" -d "${_PAYLOAD}" "${_FULL_URL}"
        else
            /opt/freeware/bin/curl -s -k -u "${OGG_USER}:${OGG_PASS}" -X "${_METHOD}" "${_FULL_URL}"
        fi
        echo ""
    elif [ -x "/usr/bin/curl" ] || [ -x "/usr/local/bin/curl" ]; then
        if [ -n "${_PAYLOAD}" ]; then
            curl -s -k -u "${OGG_USER}:${OGG_PASS}" -X "${_METHOD}" -H "Content-Type: application/json" -d "${_PAYLOAD}" "${_FULL_URL}"
        else
            curl -s -k -u "${OGG_USER}:${OGG_PASS}" -X "${_METHOD}" "${_FULL_URL}"
        fi
        echo ""
    elif [ -x "/usr/bin/perl" ]; then
        /usr/bin/perl "${WORKDIR}/ogg_rest.pl" "${_METHOD}" "${_FULL_URL}" "${OGG_USER}" "${OGG_PASS}" "${_PAYLOAD}"
    else
        echo "WARNING: Neither curl nor /usr/bin/perl found for REST API call."
    fi
}
cat << 'EOF_AWK' > "${WORKDIR}/parse_chkpt.awk"
/^(REPLICAT|EXTRACT)[[:space:]]+/ ||
/Status[[:space:]]*:/ ||
/Checkpoint Lag/ ||
/Time Since Chkpt/ ||
/Checkpoint table/ ||
/Low Watermark CSN/ ||
/High Watermark CSN/ ||
/Log Read Checkpoint/ ||
/Log Write Checkpoint/ ||
/Recovery Checkpoint/ ||
/Current Checkpoint/ ||
/Input Checkpoint/ ||
/Output Checkpoint/ ||
/Current Read Position/ ||
/Current Write Position/ ||
/^[[:space:]]*(First Record|[0-9]{4}-[0-9]{2}-[0-9]{2}|Seq|Sequence|RBA).*RBA/ {
    print "    " $0
}
EOF_AWK

cat << 'EOF_AWK' > "${WORKDIR}/get_chkpt_seq.awk"
/Log Read Checkpoint.*File/ || /Log Write Checkpoint.*File/ || /Current Checkpoint.*File/ {
    for (i=1; i<=NF; i++) {
        if ($i == "File") {
            val = $(i+1)
            sub(/^.*[A-Za-z_\/]/, "", val)
            if (val ~ /^[0-9]+$/) {
                print val + 0
                exit
            }
        }
    }
}
/Sequence[[:space:]]+[0-9]+/ {
    for (i=1; i<=NF; i++) {
        if ($i ~ /^Sequence/) {
            val = $(i+1)
            gsub(/[,;]/, "", val)
            if (val ~ /^[0-9]+$/) {
                print val + 0
                exit
            }
        }
    }
}
EOF_AWK

cat << 'EOF_AWK' > "${WORKDIR}/get_chkpt_rba.awk"
/RBA[[:space:]]+[0-9]+/ {
    for (i=1; i<=NF; i++) {
        if ($i == "RBA") {
            val = $(i+1)
            gsub(/[,;]/, "", val)
            if (val ~ /^[0-9]+$/) {
                print val
                exit
            }
        }
    }
}
EOF_AWK

cat << 'EOF_AWK' > "${WORKDIR}/get_chkpt_lag.awk"
/Checkpoint Lag/ {
    print $3
    exit
}
/Lag[[:space:]]*:/ {
    sub(/^.*:[[:space:]]*/, "")
    print $1
    exit
}
EOF_AWK

# Pre-Drop Consistency Check & Warning Banner (Prevents Transaction Loss)
function verify_before_drop {
    _PROC_TYPE="$1"
    _PROC_NAME="$2"
    _CACHED_DETAIL="$3"

    echo ""
    echo "=========================================================================="
    echo "  !!! CRITICAL WARNING: DROPPING ${_PROC_TYPE} [ ${_PROC_NAME} ] !!!"
    echo "=========================================================================="
    echo "  Dropping and recreating a ${_PROC_TYPE} (especially with BEGIN NOW)"
    echo "  WILL SKIP TRANSACTIONS in the pipeline if there is ANY lag!"
    echo ""
    echo "  1. VERIFY ZERO LAG (00:00:00) BEFORE DROPPING:"
    echo "     Wait for lag to drain completely before dropping/recreating."
    echo "  2. IF LAG OR TABLE DISCREPANCIES EXIST (Prevent Missing/Failed Txns):"
    echo "     Use MAPEXCLUDE or HANDLECOLLISIONS in your .prm file BEFORE wildcard"
    echo "     MAP statements, then remove them once lag catches up to 00:00:00:"
    echo "       Example 1 (Skip lagging table temporarily):"
    echo "         MAPEXCLUDE APP_SCHEMA.ORDERS_TABLE;"
    echo "       Example 2 (Absorb duplicate/missing row collisions on recreate):"
    echo "         MAP APP_SCHEMA.ORDERS_TABLE, TARGET APP_SCHEMA.ORDERS_TABLE, HANDLECOLLISIONS;"
    echo "  3. RECORD EXACT RBA & SEQUENCE BELOW FOR ZERO-DATA-LOSS RECOVERY:"
    echo "     Use EXTSEQNO and EXTRBA instead of BEGIN NOW if repositioning."
    echo "=========================================================================="

    _SNAPSHOT_FILE="${WORKDIR}/chkpt_snapshot_${_PROC_NAME}_${TIMESTAMP}.txt"
    if [ -n "${_CACHED_DETAIL}" ] && [ -s "${_CACHED_DETAIL}" ]; then
        cp "${_CACHED_DETAIL}" "${_SNAPSHOT_FILE}"
    else
        if [ "${_PROC_TYPE}" = "distpath" ] || [ "${_PROC_TYPE}" = "DISTPATH" ]; then
            run_ogg_cmd "${_SNAPSHOT_FILE}" "info distpath ${_PROC_NAME} detail"
        else
            run_ogg_cmd "${_SNAPSHOT_FILE}" "info ${_PROC_TYPE} ${_PROC_NAME} detail"
        fi
    fi

    awk -f "${WORKDIR}/get_chkpt_lag.awk" "${_SNAPSHOT_FILE}" > "${WORKDIR}/_clag.tmp"
    read _CHK_LAG < "${WORKDIR}/_clag.tmp"

    awk -f "${WORKDIR}/get_chkpt_seq.awk" "${_SNAPSHOT_FILE}" > "${WORKDIR}/_cseq.tmp"
    read _CHK_SEQ < "${WORKDIR}/_cseq.tmp"

    awk -f "${WORKDIR}/get_chkpt_rba.awk" "${_SNAPSHOT_FILE}" > "${WORKDIR}/_crba.tmp"
    read _CHK_RBA < "${WORKDIR}/_crba.tmp"

    awk -f "${WORKDIR}/get_rep_trail.awk" "${_SNAPSHOT_FILE}" > "${WORKDIR}/_ctrail.tmp"
    read _CHK_TRAIL < "${WORKDIR}/_ctrail.tmp"

    echo ">>> TRANSACTION CONSISTENCY & CHECKPOINT SNAPSHOT [${_PROC_NAME}]:"
    echo "    Current Lag      : ${_CHK_LAG:-<Check details below>}"
    echo "    Trail Prefix     : ${_CHK_TRAIL:-<Check details below>}"
    echo "    Sequence (SEQNO) : ${_CHK_SEQ:-<Check details below>}"
    echo "    Current RBA      : ${_CHK_RBA:-<Check details below>}"
    echo "    Snapshot File    : ${_SNAPSHOT_FILE}"
    echo "--------------------------------------------------------------------------"
    echo ">>> FULL CHECKPOINT DETAILS:"
    awk -f "${WORKDIR}/parse_chkpt.awk" "${_SNAPSHOT_FILE}"
    if [ ! -s "${_SNAPSHOT_FILE}" ]; then
        echo "    [No active detail output returned - process may already be stopped/missing]"
    fi
    echo "--------------------------------------------------------------------------"
    echo ">>> EXACT REPOSITIONING COMMANDS (To Avoid Missing Transactions):"
    if [ -n "${_CHK_SEQ}" ] && [ -n "${_CHK_RBA}" ]; then
        echo "    ALTER ${_PROC_TYPE} ${_PROC_NAME}, EXTSEQNO ${_CHK_SEQ}, EXTRBA ${_CHK_RBA}"
        if [ -n "${_CHK_TRAIL}" ]; then
            echo "    (Or recreate with: EXTTRAIL ${_CHK_TRAIL}, EXTSEQNO ${_CHK_SEQ}, EXTRBA ${_CHK_RBA})"
        fi
    else
        echo "    ALTER ${_PROC_TYPE} ${_PROC_NAME}, EXTSEQNO <seq_no>, EXTRBA <rba_no>"
    fi
    echo "=========================================================================="

    printf "Confirm ZERO LAG & proceed to DROP %s %s? [Type YES to confirm]: " "${_PROC_TYPE}" "${_PROC_NAME}"
    read RAW_CONFIRM < /dev/tty
    CONFIRM_ACT=`echo "${RAW_CONFIRM}" | tr -d '\015 \t '`
    if [ "${CONFIRM_ACT}" != "YES" ]; then
        echo ">>> Aborted drop for ${_PROC_NAME} (did not enter YES)."
        return 1
    fi
    return 0
}

# ------------------------------------------------------------------------------
# 4. Interactive Connection & Deployment Menu
# ------------------------------------------------------------------------------
echo "=========================================================================="
echo " ORACLE GOLDENGATE MICROSERVICES PIPELINE MANAGER (v${SCRIPT_VERSION})"
echo "=========================================================================="

printf "Enter OGG URL [Default: %s]: " "${OGG_URL}"
read RAW_URL < /dev/tty
TMP_URL=`echo "${RAW_URL}" | tr -d '\015 \t '`
[ -n "${TMP_URL}" ] && OGG_URL="${TMP_URL}"

printf "Enter OGG User [Default: %s]: " "${OGG_USER}"
read RAW_USER < /dev/tty
TMP_USER=`echo "${RAW_USER}" | tr -d '\015 \t '`
[ -n "${TMP_USER}" ] && OGG_USER="${TMP_USER}"

printf "Enter OGG Password [Press Enter to keep current]: "
stty -echo
read RAW_PASS < /dev/tty
stty echo
echo ""
TMP_PASS=`echo "${RAW_PASS}" | tr -d '\015 \t '`
[ -n "${TMP_PASS}" ] && OGG_PASS="${TMP_PASS}"

echo "=========================================================================="
echo " Fetching available deployments from ${OGG_URL}..."
echo "=========================================================================="

run_ogg_sm_cmd "${WORKDIR}/deps_raw.log"

grep -E -i "^ERROR:|Not logged in" "${WORKDIR}/deps_raw.log" | grep -v -i "Deployment name must be specified" | grep -v -i "Unexpected value" > "${WORKDIR}/fatal_errors.log"

if [ -s "${WORKDIR}/fatal_errors.log" ]; then
    echo ">>> STATUS: UNSUCCESSFUL"
    echo "Failed to connect to ${OGG_URL}."
    cat "${WORKDIR}/deps_raw.log"
    exit 1
fi

awk -f "${WORKDIR}/parse_deps.awk" "${WORKDIR}/deps_raw.log" > "${WORKDIR}/dep_list.txt"

if [ ! -s "${WORKDIR}/dep_list.txt" ]; then
    echo "WARNING: Could not parse deployments automatically from OGG output."
    echo "Raw Output from Service Manager:"
    cat "${WORKDIR}/deps_raw.log"
    echo ""
    if [ -n "${DEPLOYMENT}" ]; then
        printf "Enter Deployment Name Manually [Default: %s]: " "${DEPLOYMENT}"
    else
        printf "Enter Deployment Name Manually: "
    fi
    read RAW_DEP < /dev/tty
    TMP_DEP=`echo "${RAW_DEP}" | tr -d '\015 \t '`
    [ -n "${TMP_DEP}" ] && DEPLOYMENT="${TMP_DEP}"
else
    echo ">>> Available Deployments:"
    IDX=1
    while read DEP_NAME; do
        echo "  ${IDX}] ${DEP_NAME}"
        IDX=`expr $IDX + 1`
    done < "${WORKDIR}/dep_list.txt"
    
    echo ""
    MAX_SEL=`expr $IDX - 1`
    printf "Select Deployment [1-%s]: " "${MAX_SEL}"
    read DEP_SEL < /dev/tty
    DEP_SEL=`echo "${DEP_SEL}" | tr -d '\015 \t '`
    
    SELECTED_DEP=`awk -v sel="${DEP_SEL}" 'NR==sel {print $1}' "${WORKDIR}/dep_list.txt"`
    if [ -n "${SELECTED_DEP}" ]; then
        DEPLOYMENT="${SELECTED_DEP}"
    else
        echo "Invalid selection. Exiting."
        exit 1
    fi
fi

echo ">>> STATUS: SUCCESS"
echo "Selected Deployment locked as: \"${DEPLOYMENT}\""
echo ""

# ------------------------------------------------------------------------------
# 5. Verify Connection to Selected Deployment
# ------------------------------------------------------------------------------
echo "Verifying deployment access..."
run_ogg_cmd "${WORKDIR}/conn_test.log" "info all"

grep -E -i "^ERROR:|Not logged in" "${WORKDIR}/conn_test.log" > /dev/null
if [ $? -eq 0 ]; then
    echo ">>> STATUS: UNSUCCESSFUL"
    echo "Failed to log into deployment \"${DEPLOYMENT}\"."
    cat "${WORKDIR}/conn_test.log"
    exit 1
fi
echo "Access Verified."
echo ""

# ------------------------------------------------------------------------------
# 6. Interactive Menu Loop
# ------------------------------------------------------------------------------
while true; do
    echo "=========================================================================="
    echo " OGG PIPELINE MANAGER v${SCRIPT_VERSION} [Deployment: ${DEPLOYMENT}]"
    echo " Working Dir: ${WORKDIR}"
    echo "========================================================================================="
    echo " [1] List Replicats   [4] Stop Process     [7] Create Process    [10] Purge Tasks"
    echo " [2] List Extracts    [5] Drop Process     [8] Dist Paths Mgt    [11] Switch Deployment"
    echo " [3] Start Process    [6] Drop/Recreate    [9] DB Connections    [12] Exit"
    echo "========================================================================================="
    printf "Select an option [1-12]: "
    read MENU_OPT < /dev/tty
    MENU_OPT=`echo "${MENU_OPT}" | tr -d '\015 \t '`
    echo ""

    case "${MENU_OPT}" in
        1)
            echo ">>> Discovered Replicats:"
            run_ogg_cmd "${WORKDIR}/list_rep.log" "info all"
            grep -E "^REPLICAT " "${WORKDIR}/list_rep.log" | sed "s/^/    /"
            echo ""
            ;;
        2)
            echo ">>> Discovered Extracts:"
            run_ogg_cmd "${WORKDIR}/list_ext.log" "info all"
            grep -E "^EXTRACT " "${WORKDIR}/list_ext.log" | sed "s/^/    /"
            echo ""
            ;;
        3)
            run_ogg_cmd "${WORKDIR}/list_start.log" "info all"

            echo "Select Process Kind to Start:"
            echo "  1] REPLICAT"
            echo "  2] EXTRACT"
            printf "Selection [1-2]: "
            read KIND_OPT < /dev/tty
            KIND_OPT=`echo "${KIND_OPT}" | tr -d '\015 \t '`
            [ "${KIND_OPT}" = "1" ] && P_KIND="REPLICAT" || P_KIND="EXTRACT"

            echo ""
            echo ">>> Available ${P_KIND}s in Deployment:"
            grep -E "^${P_KIND} " "${WORKDIR}/list_start.log" | sed "s/^/    /"
            echo ""

            printf "Enter Process Name [* for all]: "
            read P_NAME < /dev/tty
            P_NAME=`echo "${P_NAME}" | tr -d '\015 \t '`
            
            echo "Starting ${P_KIND} ${P_NAME}..."
            run_ogg_cmd STDOUT "start ${P_KIND} ${P_NAME}" "info ${P_KIND} ${P_NAME}"
            echo ""
            ;;
        4)
            run_ogg_cmd "${WORKDIR}/list_stop.log" "info all"

            echo "Select Process Kind to Stop:"
            echo "  1] REPLICAT"
            echo "  2] EXTRACT"
            printf "Selection [1-2]: "
            read KIND_OPT < /dev/tty
            KIND_OPT=`echo "${KIND_OPT}" | tr -d '\015 \t '`
            [ "${KIND_OPT}" = "1" ] && P_KIND="REPLICAT" || P_KIND="EXTRACT"

            echo ""
            echo ">>> Available ${P_KIND}s in Deployment:"
            grep -E "^${P_KIND} " "${WORKDIR}/list_stop.log" | sed "s/^/    /"
            echo ""

            printf "Enter Process Name [* for all]: "
            read P_NAME < /dev/tty
            P_NAME=`echo "${P_NAME}" | tr -d '\015 \t '`
            
            echo "Stopping ${P_KIND} ${P_NAME}..."
            run_ogg_cmd STDOUT "stop ${P_KIND} ${P_NAME}" "info ${P_KIND} ${P_NAME}"
            echo ""
            ;;
        5)
            # ==================================================================
            # DROP PROCESS ONLY
            # ==================================================================
            echo ">>> Fetching deployment process list..."
            run_ogg_cmd "${WORKDIR}/all_procs_backup.log" "info all"

            echo "Select Process Kind to Drop:"
            echo "  1] REPLICAT"
            echo "  2] EXTRACT"
            printf "Selection [1-2]: "
            read KIND_OPT < /dev/tty
            KIND_OPT=`echo "${KIND_OPT}" | tr -d '\015 \t '`
            if [ "${KIND_OPT}" = "1" ]; then
                P_KIND="REPLICAT"
            elif [ "${KIND_OPT}" = "2" ]; then
                P_KIND="EXTRACT"
            else
                echo "Invalid selection. Returning to menu."
                continue
            fi

            echo ""
            echo ">>> Available ${P_KIND}s in Deployment:"
            grep -E "^${P_KIND} " "${WORKDIR}/all_procs_backup.log" | sed "s/^/    /"
            echo ""

            if [ "${P_KIND}" = "REPLICAT" ]; then
                printf "Enter Name [e.g. R01], Wildcard [e.g. R*], Comma-Separated [e.g. R01,R02], or ALL: "
            else
                printf "Enter Name [e.g. E01], Wildcard [e.g. E*], Comma-Separated [e.g. E01,E02], or ALL: "
            fi
            read RAW_TGT < /dev/tty
            
            echo "${RAW_TGT}" | tr -d '\015\t ' | tr ',' '\n' > "${WORKDIR}/_tgt_patterns.tmp"
            cat /dev/null > "${WORKDIR}/target_list.txt"

            while read _PAT; do
                [ -z "${_PAT}" ] && continue
                _MATCH_COUNT=0
                
                if [ "${_PAT}" = "ALL" ] || [ "${_PAT}" = "all" ] || [ "${_PAT}" = "*" ]; then
                    while read _PROC_KIND _PROC_STATUS _PROC_NAME _REST; do
                        if [ "${_PROC_KIND}" = "${P_KIND}" ]; then
                            echo "${_PROC_NAME}" >> "${WORKDIR}/target_list.txt"
                            _MATCH_COUNT=1
                        fi
                    done < "${WORKDIR}/all_procs_backup.log"
                else
                    while read _PROC_KIND _PROC_STATUS _PROC_NAME _REST; do
                        if [ "${_PROC_KIND}" = "${P_KIND}" ]; then
                            case "${_PROC_NAME}" in
                                ${_PAT}) 
                                    echo "${_PROC_NAME}" >> "${WORKDIR}/target_list.txt" 
                                    _MATCH_COUNT=1
                                    ;;
                            esac
                        fi
                    done < "${WORKDIR}/all_procs_backup.log"

                    case "${_PAT}" in
                        *\**) ;; 
                        *)
                            if [ ${_MATCH_COUNT} -eq 0 ]; then
                                echo "WARNING: Process [${_PAT}] not found in current deployment."
                                printf "Do you want to process it for Drop/Cleanup anyway? [y/n]: "
                                read RAW_REC_MISSING < /dev/tty
                                CONFIRM_REC_MISSING=`echo "${RAW_REC_MISSING}" | tr -d '\015 \t '`
                                case "${CONFIRM_REC_MISSING}" in
                                    [yY]) echo "${_PAT}" >> "${WORKDIR}/target_list.txt" ;;
                                esac
                            fi
                            ;;
                    esac
                fi
            done < "${WORKDIR}/_tgt_patterns.tmp"

            awk '!seen[$0]++' "${WORKDIR}/target_list.txt" > "${WORKDIR}/_tmp_list.txt"
            cat "${WORKDIR}/_tmp_list.txt" > "${WORKDIR}/target_list.txt"

            if [ ! -s "${WORKDIR}/target_list.txt" ]; then
                echo "WARNING: No ${P_KIND} processes selected/found."
                echo ""
                continue
            fi

            while read P_NAME; do
                echo "--------------------------------------------------------------------------"
                
                grep "^${P_KIND} " "${WORKDIR}/all_procs_backup.log" | grep " ${P_NAME} " > /dev/null
                _PROC_EXISTS_RC=$?

                PRM_FILE="${PRM_BACKUP_DIR}/${P_NAME}.prm"
                if [ ! -s "${PRM_FILE}" ]; then
                    echo "--> Saving parameter file backup to ${PRM_FILE} before Drop..."
                    run_ogg_cmd "${WORKDIR}/${P_NAME}_prm.raw" "view params ${P_NAME}"
                    awk -f "${WORKDIR}/extract_prm.awk" "${WORKDIR}/${P_NAME}_prm.raw" > "${PRM_FILE}"
                fi
                
                awk -f "${WORKDIR}/get_alias.awk" "${PRM_FILE}" > "${WORKDIR}/_alias.tmp"
                read CRED_ALIAS < "${WORKDIR}/_alias.tmp"

                awk -f "${WORKDIR}/get_domain.awk" "${PRM_FILE}" > "${WORKDIR}/_domain.tmp"
                read CRED_DOMAIN < "${WORKDIR}/_domain.tmp"
                [ -z "${CRED_DOMAIN}" ] && CRED_DOMAIN="OracleGoldenGate"

                if [ -z "${CRED_ALIAS}" ]; then
                    printf "WARNING: Could not automatically read DB alias from .prm file for %s. Enter DB Alias manually: " "${P_NAME}"
                    read RAW_ALIAS < /dev/tty
                    CRED_ALIAS=`echo "${RAW_ALIAS}" | tr -d '\015 \t '`
                fi

                echo ""
                echo ">>> PREVIEW: Planned Execution for DROP of ${P_KIND} ${P_NAME}"
                if [ ${_PROC_EXISTS_RC} -eq 0 ]; then
                    echo "    Command 1 : stop ${P_KIND} ${P_NAME}"
                    echo "    Command 2 : unregister ${P_KIND} ${P_NAME} database"
                    echo "    Command 3 : delete ${P_KIND} ${P_NAME}"
                else
                    echo "    [Process is missing from deployment. Executing cleanup.]"
                    echo "    Command 1 : unregister ${P_KIND} ${P_NAME} database"
                    echo "    Command 2 : delete ${P_KIND} ${P_NAME}"
                fi
                echo ""

                verify_before_drop "${P_KIND}" "${P_NAME}" ""
                if [ $? -eq 0 ]; then
                    echo ">>> Executing Drop for ${P_NAME}..."
                    if [ ${_PROC_EXISTS_RC} -eq 0 ]; then
                        run_ogg_cmd STDOUT \
                            "dblogin useridalias ${CRED_ALIAS} domain ${CRED_DOMAIN}" \
                            "stop ${P_KIND} ${P_NAME}" \
                            "unregister ${P_KIND} ${P_NAME} database" \
                            "delete ${P_KIND} ${P_NAME}"
                    else
                        run_ogg_cmd STDOUT \
                            "dblogin useridalias ${CRED_ALIAS} domain ${CRED_DOMAIN}" \
                            "unregister ${P_KIND} ${P_NAME} database" \
                            "delete ${P_KIND} ${P_NAME}"
                    fi
                else
                    echo "Skipped ${P_NAME}."
                fi
            done < "${WORKDIR}/target_list.txt"
            echo "--------------------------------------------------------------------------"
            echo "Batch Drop Phase Complete."
            echo ""
            
            echo ">>> Current Processes Status:"
            run_ogg_cmd "${WORKDIR}/list_final.log" "info all"
            grep -E "^REPLICAT |^EXTRACT " "${WORKDIR}/list_final.log" | sed "s/^/    /"
            echo ""
            ;;
        6)
            # ==================================================================
            # DROP AND RECREATE PROCESS
            # ==================================================================
            echo ">>> Fetching deployment process list..."
            run_ogg_cmd "${WORKDIR}/all_procs_backup.log" "info all"

            printf "Backup ALL Replicat and Extract parameter files first? [y/n]: "
            read RAW_ALL_BKP < /dev/tty
            CONFIRM_ALL_BKP=`echo "${RAW_ALL_BKP}" | tr -d '\015 \t '`
            
            case "${CONFIRM_ALL_BKP}" in
                [yY])
                    echo ">>> Backing up ALL parameter files..."
                    grep -E "^REPLICAT |^EXTRACT " "${WORKDIR}/all_procs_backup.log" > "${WORKDIR}/all_names_raw.txt"
                    
                    BKP_COUNT=0
                    while read _KIND _STATUS _BKP_NAME _REST; do
                        run_ogg_cmd "${WORKDIR}/${_BKP_NAME}_prm.raw" "view params ${_BKP_NAME}"
                        awk -f "${WORKDIR}/extract_prm.awk" "${WORKDIR}/${_BKP_NAME}_prm.raw" > "${PRM_BACKUP_DIR}/${_BKP_NAME}.prm"
                        if [ -s "${PRM_BACKUP_DIR}/${_BKP_NAME}.prm" ]; then
                            BKP_COUNT=`expr $BKP_COUNT + 1`
                        fi
                    done < "${WORKDIR}/all_names_raw.txt"
                    
                    echo "  [OK] Successfully backed up ${BKP_COUNT} parameter files to ${PRM_BACKUP_DIR}"
                    echo ""
                    ;;
                *)
                    echo "Skipped global backup. [Selected targets will be backed up on-the-fly]."
                    echo ""
                    ;;
            esac

            echo "Select Process Kind to Drop/Recreate:"
            echo "  1] REPLICAT"
            echo "  2] EXTRACT"
            printf "Selection [1-2]: "
            read KIND_OPT < /dev/tty
            KIND_OPT=`echo "${KIND_OPT}" | tr -d '\015 \t '`
            if [ "${KIND_OPT}" = "1" ]; then
                P_KIND="REPLICAT"
            elif [ "${KIND_OPT}" = "2" ]; then
                P_KIND="EXTRACT"
            else
                echo "Invalid selection. Returning to menu."
                continue
            fi

            echo ""
            echo ">>> Available ${P_KIND}s in Deployment:"
            grep -E "^${P_KIND} " "${WORKDIR}/all_procs_backup.log" | sed "s/^/    /"
            echo ""

            if [ "${P_KIND}" = "REPLICAT" ]; then
                printf "Enter Name [e.g. R01], Wildcard [e.g. R*], Comma-Separated [e.g. R01,R02], or ALL: "
            else
                printf "Enter Name [e.g. E01], Wildcard [e.g. E*], Comma-Separated [e.g. E01,E02], or ALL: "
            fi
            read RAW_TGT < /dev/tty
            
            echo "${RAW_TGT}" | tr -d '\015\t ' | tr ',' '\n' > "${WORKDIR}/_tgt_patterns.tmp"
            cat /dev/null > "${WORKDIR}/target_list.txt"

            while read _PAT; do
                [ -z "${_PAT}" ] && continue
                _MATCH_COUNT=0
                
                if [ "${_PAT}" = "ALL" ] || [ "${_PAT}" = "all" ] || [ "${_PAT}" = "*" ]; then
                    while read _PROC_KIND _PROC_STATUS _PROC_NAME _REST; do
                        if [ "${_PROC_KIND}" = "${P_KIND}" ]; then
                            echo "${_PROC_NAME}" >> "${WORKDIR}/target_list.txt"
                            _MATCH_COUNT=1
                        fi
                    done < "${WORKDIR}/all_procs_backup.log"
                else
                    while read _PROC_KIND _PROC_STATUS _PROC_NAME _REST; do
                        if [ "${_PROC_KIND}" = "${P_KIND}" ]; then
                            case "${_PROC_NAME}" in
                                ${_PAT}) 
                                    echo "${_PROC_NAME}" >> "${WORKDIR}/target_list.txt" 
                                    _MATCH_COUNT=1
                                    ;;
                            esac
                        fi
                    done < "${WORKDIR}/all_procs_backup.log"

                    case "${_PAT}" in
                        *\**) ;; 
                        *)
                            if [ ${_MATCH_COUNT} -eq 0 ]; then
                                echo "WARNING: Process [${_PAT}] not found in current deployment."
                                printf "Do you want to process it anyway using its existing parameter file? [y/n]: "
                                read RAW_REC_MISSING < /dev/tty
                                CONFIRM_REC_MISSING=`echo "${RAW_REC_MISSING}" | tr -d '\015 \t '`
                                case "${CONFIRM_REC_MISSING}" in
                                    [yY]) echo "${_PAT}" >> "${WORKDIR}/target_list.txt" ;;
                                esac
                            fi
                            ;;
                    esac
                fi
            done < "${WORKDIR}/_tgt_patterns.tmp"

            awk '!seen[$0]++' "${WORKDIR}/target_list.txt" > "${WORKDIR}/_tmp_list.txt"
            cat "${WORKDIR}/_tmp_list.txt" > "${WORKDIR}/target_list.txt"

            if [ ! -s "${WORKDIR}/target_list.txt" ]; then
                echo "WARNING: No ${P_KIND} processes selected/found."
                echo ""
                continue
            fi

            while read P_NAME; do
                echo "--------------------------------------------------------------------------"
                
                grep "^${P_KIND} " "${WORKDIR}/all_procs_backup.log" | grep " ${P_NAME} " > /dev/null
                _PROC_EXISTS_RC=$?

                PRM_FILE="${PRM_BACKUP_DIR}/${P_NAME}.prm"
                if [ ! -s "${PRM_FILE}" ]; then
                    echo "--> Backing up parameters for ${P_NAME}..."
                    run_ogg_cmd "${WORKDIR}/${P_NAME}_prm.raw" "view params ${P_NAME}"
                    awk -f "${WORKDIR}/extract_prm.awk" "${WORKDIR}/${P_NAME}_prm.raw" > "${PRM_FILE}"
                fi
                
                awk -f "${WORKDIR}/get_alias.awk" "${PRM_FILE}" > "${WORKDIR}/_alias.tmp"
                read CRED_ALIAS < "${WORKDIR}/_alias.tmp"

                awk -f "${WORKDIR}/get_domain.awk" "${PRM_FILE}" > "${WORKDIR}/_domain.tmp"
                read CRED_DOMAIN < "${WORKDIR}/_domain.tmp"
                [ -z "${CRED_DOMAIN}" ] && CRED_DOMAIN="OracleGoldenGate"

                if [ -z "${CRED_ALIAS}" ]; then
                    printf "WARNING: Could not automatically read DB alias from .prm file for %s. Enter DB Alias manually: " "${P_NAME}"
                    read RAW_ALIAS < /dev/tty
                    CRED_ALIAS=`echo "${RAW_ALIAS}" | tr -d '\015 \t '`
                fi

                if [ "${P_KIND}" = "REPLICAT" ]; then
                    if [ ${_PROC_EXISTS_RC} -eq 0 ]; then
                        run_ogg_cmd "${WORKDIR}/detail_${P_NAME}.log" "info ${P_NAME} detail"
                        
                        awk -f "${WORKDIR}/get_mode.awk" "${WORKDIR}/detail_${P_NAME}.log" > "${WORKDIR}/_mode.tmp"
                        read P_MODE_RAW < "${WORKDIR}/_mode.tmp"
                        
                        awk -f "${WORKDIR}/get_rep_trail.awk" "${WORKDIR}/detail_${P_NAME}.log" > "${WORKDIR}/_reptrail.tmp"
                        read DYN_TRAIL < "${WORKDIR}/_reptrail.tmp"
                    else
                        DYN_TRAIL=""
                        echo ""
                        echo "=========================================================================="
                        echo " REQUIRED: Replicat [${P_NAME}] is not currently running/registered."
                        echo " What type of Replicat should be created?"
                        echo "  1] Parallel Replicat"
                        echo "  2] Integrated Replicat"
                        echo "  3] Non-integrated [Classic] Replicat"
                        echo "  4] Coordinated Replicat"
                        echo "=========================================================================="
                        printf "Selection [1-4]: "
                        read RAW_RTYPE < /dev/tty
                        USER_RTYPE=`echo "${RAW_RTYPE}" | tr -d '\015 \t '`
                        
                        if [ "${USER_RTYPE}" = "1" ]; then
                            printf "Is this Parallel Replicat INTEGRATED? [y/n]: "
                            read RAW_RINT < /dev/tty
                            USER_RINT=`echo "${RAW_RINT}" | tr -d '\015 \t '`
                            case "${USER_RINT}" in
                                [yY]) P_MODE_RAW="PARALLEL INT" ;;
                                *)    P_MODE_RAW="PARALLEL" ;;
                            esac
                        elif [ "${USER_RTYPE}" = "2" ]; then
                            P_MODE_RAW="INTEGRATED"
                        elif [ "${USER_RTYPE}" = "4" ]; then
                            P_MODE_RAW="COORDINATED"
                        else
                            P_MODE_RAW="CLASSIC"
                        fi
                    fi
                    
                    if [ -z "${DYN_TRAIL}" ]; then
                        printf "Enter Trail File Name for Replicat %s [e.g. ./dirdat/tr]: " "${P_NAME}"
                        read RAW_TRAIL < /dev/tty
                        DYN_TRAIL=`echo "${RAW_TRAIL}" | tr -d '\015 \t '`
                    fi
                    
                    [ -z "${DYN_TRAIL}" ] && DYN_TRAIL="${TRAIL_NAME}"
                    
                    awk -f "${WORKDIR}/get_chkpt.awk" "${PRM_FILE}" > "${WORKDIR}/_chkpt.tmp"
                    read CHKPT_TABLE < "${WORKDIR}/_chkpt.tmp"
                    [ -z "${CHKPT_TABLE}" ] && CHKPT_TABLE="GGADMIN.CHKPT_TBL"

                    case "${P_MODE_RAW}" in
                        "PARALLEL INT") 
                            ADD_SYNTAX="parallel integrated, exttrail ${DYN_TRAIL}, begin now, checkpointtable ${CHKPT_TABLE}"
                            DISP_TYPE="PARALLEL INTEGRATED - Parallel parameters remain in the .prm file"
                            ;;
                        "INTEGRATED")   
                            ADD_SYNTAX="integrated, exttrail ${DYN_TRAIL}, begin now, checkpointtable ${CHKPT_TABLE}"
                            DISP_TYPE="INTEGRATED"
                            ;;
                        "PARALLEL")     
                            ADD_SYNTAX="parallel, exttrail ${DYN_TRAIL}, begin now, checkpointtable ${CHKPT_TABLE}"
                            DISP_TYPE="PARALLEL NON-INTEGRATED - Parallel parameters remain in the .prm file"
                            ;;
                        "COORDINATED")     
                            ADD_SYNTAX="coordinated, exttrail ${DYN_TRAIL}, begin now, checkpointtable ${CHKPT_TABLE}"
                            DISP_TYPE="COORDINATED"
                            ;;
                        *)              
                            ADD_SYNTAX="exttrail ${DYN_TRAIL}, begin now, checkpointtable ${CHKPT_TABLE}"
                            DISP_TYPE="CLASSIC [Non-integrated]"
                            ;;
                    esac
                else
                    awk -f "${WORKDIR}/get_exttrail.awk" "${PRM_FILE}" > "${WORKDIR}/_exttrail.tmp"
                    read EXT_TRAIL < "${WORKDIR}/_exttrail.tmp"
                    [ -z "${EXT_TRAIL}" ] && EXT_TRAIL="${TRAIL_NAME}"
                    
                    ADD_SYNTAX="integrated tranlog, begin now"
                    EXTTRAIL_CMD="add exttrail ${EXT_TRAIL}, extract ${P_NAME}"
                    
                    awk -f "${WORKDIR}/get_pdb.awk" "${PRM_FILE}" > "${WORKDIR}/_pdb.tmp"
                    read EXT_PDB < "${WORKDIR}/_pdb.tmp"
                    
                    echo ""
                    echo "=========================================================================="
                    echo " MULTITENANT REGISTRATION FOR EXTRACT [${P_NAME}]"
                    echo " Integrated Extracts must be explicitly registered to their specific PDB."
                    echo "=========================================================================="
                    if [ -z "${EXT_PDB}" ]; then
                        printf "Enter PDB Container [e.g. PDB01] or press Enter for CDB\$ROOT: "
                    else
                        printf "Enter PDB Container [Press Enter to keep: %s]: " "${EXT_PDB}"
                    fi
                    read RAW_PDB < /dev/tty
                    USER_PDB=`echo "${RAW_PDB}" | tr -d '\015 \t '`
                    
                    if [ -n "${USER_PDB}" ]; then
                        if [ "${USER_PDB}" = "NONE" ] || [ "${USER_PDB}" = "none" ] || [ "${USER_PDB}" = "CDB\$ROOT" ]; then
                            EXT_PDB=""
                        else
                            EXT_PDB="${USER_PDB}"
                        fi
                    fi
                    
                    if [ -n "${EXT_PDB}" ]; then
                        PDB_STR="(${EXT_PDB})"
                        REG_CMD="register ${P_KIND} ${P_NAME} database container ${PDB_STR}"
                        DISP_TYPE="INTEGRATED EXTRACT [Container: ${EXT_PDB}]"
                    else
                        REG_CMD="register ${P_KIND} ${P_NAME} database"
                        DISP_TYPE="INTEGRATED EXTRACT [CDB\$ROOT / No Container]"
                    fi
                fi
                
                echo ""
                echo ">>> PREVIEW: Planned Execution for ${P_KIND} ${P_NAME}"
                echo "    Detected Type : ${DISP_TYPE}"
                
                if [ ${_PROC_EXISTS_RC} -eq 0 ]; then
                    echo "    Command 1 : stop ${P_KIND} ${P_NAME}"
                    echo "    Command 2 : unregister ${P_KIND} ${P_NAME} database"
                    echo "    Command 3 : delete ${P_KIND} ${P_NAME}"
                    if [ "${P_KIND}" = "REPLICAT" ]; then
                        echo "    Command 4 : add ${P_KIND} ${P_NAME}, ${ADD_SYNTAX}"
                        echo "    Command 5 : register ${P_KIND} ${P_NAME} database"
                    else
                        echo "    Command 4 : add ${P_KIND} ${P_NAME}, ${ADD_SYNTAX}"
                        echo "    Command 5 : ${REG_CMD}"
                        echo "    Command 6 : ${EXTTRAIL_CMD}"
                    fi
                else
                    echo "    [Process is missing from deployment. Skipping stop/unregister/delete.]"
                    if [ "${P_KIND}" = "REPLICAT" ]; then
                        echo "    Command 1 : add ${P_KIND} ${P_NAME}, ${ADD_SYNTAX}"
                        echo "    Command 2 : register ${P_KIND} ${P_NAME} database"
                    else
                        echo "    Command 1 : add ${P_KIND} ${P_NAME}, ${ADD_SYNTAX}"
                        echo "    Command 2 : ${REG_CMD}"
                        echo "    Command 3 : ${EXTTRAIL_CMD}"
                    fi
                fi
                echo ""

                verify_before_drop "${P_KIND}" "${P_NAME}" "${WORKDIR}/detail_${P_NAME}.log"
                if [ $? -eq 0 ]; then
                    echo ">>> Executing Drop and Recreate for ${P_NAME}..."
                    if [ "${P_KIND}" = "REPLICAT" ]; then
                        if [ ${_PROC_EXISTS_RC} -eq 0 ]; then
                            run_ogg_cmd STDOUT \
                                "dblogin useridalias ${CRED_ALIAS} domain ${CRED_DOMAIN}" \
                                "stop ${P_KIND} ${P_NAME}" \
                                "unregister ${P_KIND} ${P_NAME} database" \
                                "delete ${P_KIND} ${P_NAME}" \
                                "add ${P_KIND} ${P_NAME}, ${ADD_SYNTAX}" \
                                "register ${P_KIND} ${P_NAME} database" \
                                "info ${P_KIND} ${P_NAME}"
                        else
                            run_ogg_cmd STDOUT \
                                "dblogin useridalias ${CRED_ALIAS} domain ${CRED_DOMAIN}" \
                                "add ${P_KIND} ${P_NAME}, ${ADD_SYNTAX}" \
                                "register ${P_KIND} ${P_NAME} database" \
                                "info ${P_KIND} ${P_NAME}"
                        fi
                    else
                        if [ ${_PROC_EXISTS_RC} -eq 0 ]; then
                            run_ogg_cmd STDOUT \
                                "dblogin useridalias ${CRED_ALIAS} domain ${CRED_DOMAIN}" \
                                "stop ${P_KIND} ${P_NAME}" \
                                "unregister ${P_KIND} ${P_NAME} database" \
                                "delete ${P_KIND} ${P_NAME}" \
                                "add ${P_KIND} ${P_NAME}, ${ADD_SYNTAX}" \
                                "${REG_CMD}" \
                                "${EXTTRAIL_CMD}" \
                                "info ${P_KIND} ${P_NAME}"
                        else
                            run_ogg_cmd STDOUT \
                                "dblogin useridalias ${CRED_ALIAS} domain ${CRED_DOMAIN}" \
                                "add ${P_KIND} ${P_NAME}, ${ADD_SYNTAX}" \
                                "${REG_CMD}" \
                                "${EXTTRAIL_CMD}" \
                                "info ${P_KIND} ${P_NAME}"
                        fi
                    fi
                    
                    echo ""
                    printf "Do you want to START the newly created %s %s? [y/n]: " "${P_KIND}" "${P_NAME}"
                    read RAW_START < /dev/tty
                    CONFIRM_START=`echo "${RAW_START}" | tr -d '\015 \t '`
                    case "${CONFIRM_START}" in
                        [yY])
                            echo "Starting ${P_KIND} ${P_NAME}..."
                            run_ogg_cmd STDOUT "start ${P_KIND} ${P_NAME}" "info ${P_KIND} ${P_NAME}"
                            ;;
                        *)
                            echo "Process ${P_NAME} remains stopped."
                            ;;
                    esac
                else
                    echo "Skipped ${P_NAME}."
                fi
            done < "${WORKDIR}/target_list.txt"
            echo "--------------------------------------------------------------------------"
            echo "Batch Recreate Phase Complete."
            echo ""
            
            echo ">>> Current Processes Status:"
            run_ogg_cmd "${WORKDIR}/list_final.log" "info all"
            grep -E "^REPLICAT |^EXTRACT " "${WORKDIR}/list_final.log" | sed "s/^/    /"
            echo ""
            ;;
        7)
            # ==================================================================
            # CREATE NEW PROCESS
            # ==================================================================
            echo "Select Process Kind to Create:"
            echo "  1] REPLICAT"
            echo "  2] EXTRACT"
            printf "Selection [1-2]: "
            read KIND_OPT < /dev/tty
            KIND_OPT=`echo "${KIND_OPT}" | tr -d '\015 \t '`
            [ "${KIND_OPT}" = "1" ] && P_KIND="REPLICAT" || P_KIND="EXTRACT"
            
            printf "Enter Process Name to Create: "
            read P_NAME < /dev/tty
            P_NAME=`echo "${P_NAME}" | tr -d '\015 \t '`
            
            printf "Enter DB Credential Alias [e.g. PDB_SRC01]: "
            read RAW_ALIAS < /dev/tty
            CRED_ALIAS=`echo "${RAW_ALIAS}" | tr -d '\015 \t '`
            
            printf "Enter DB Credential Domain [Press Enter for OracleGoldenGate]: "
            read RAW_DOMAIN < /dev/tty
            CRED_DOMAIN=`echo "${RAW_DOMAIN}" | tr -d '\015 \t '`
            [ -z "${CRED_DOMAIN}" ] && CRED_DOMAIN="OracleGoldenGate"

            if [ "${P_KIND}" = "EXTRACT" ]; then
                echo ""
                echo "=========================================================================="
                echo " MULTITENANT REGISTRATION FOR NEW EXTRACT"
                echo " Integrated Extracts must be registered to their specific PDB."
                echo "=========================================================================="
                printf "Enter PDB Container [e.g. PDB01] or press Enter for CDB\$ROOT: "
                read RAW_PDB < /dev/tty
                EXT_PDB=`echo "${RAW_PDB}" | tr -d '\015 \t '`
                if [ -n "${EXT_PDB}" ]; then
                    PDB_STR="(${EXT_PDB})"
                    REG_CMD="register ${P_KIND} ${P_NAME} database container ${PDB_STR}"
                else
                    REG_CMD="register ${P_KIND} ${P_NAME} database"
                fi
                echo ""
                echo "Preview Registration Command: ${REG_CMD}"
            fi

            echo ""
            echo "Enter the configuration syntax after the process name."
            if [ "${P_KIND}" = "REPLICAT" ]; then
                echo "[e.g.: parallel integrated, exttrail ${TRAIL_NAME}, begin now, checkpointtable GGADMIN.CHKPT_TBL]"
            else
                echo "[e.g.: integrated tranlog, begin now]"
            fi
            printf "Syntax: "
            read RAW_SYNTAX < /dev/tty
            ADD_SYNTAX=`echo "${RAW_SYNTAX}" | tr -d '\015'`

            echo ""
            printf "PERMISSION REQUIRED: Do you want to create and register %s %s now? [y/n]: " "${P_KIND}" "${P_NAME}"
            read RAW_CREATE < /dev/tty
            CONFIRM_CREATE=`echo "${RAW_CREATE}" | tr -d '\015 \t '`
            case "${CONFIRM_CREATE}" in
                [yY])
                    echo "Creating ${P_NAME}..."
                    if [ "${P_KIND}" = "REPLICAT" ]; then
                        run_ogg_cmd STDOUT \
                            "dblogin useridalias ${CRED_ALIAS} domain ${CRED_DOMAIN}" \
                            "add ${P_KIND} ${P_NAME}, ${ADD_SYNTAX}" \
                            "register ${P_KIND} ${P_NAME} database" \
                            "info ${P_KIND} ${P_NAME} detail"
                    else
                        run_ogg_cmd STDOUT \
                            "dblogin useridalias ${CRED_ALIAS} domain ${CRED_DOMAIN}" \
                            "add ${P_KIND} ${P_NAME}, ${ADD_SYNTAX}" \
                            "${REG_CMD}" \
                            "info ${P_KIND} ${P_NAME} detail"
                    fi
                        
                    echo ""
                    printf "Do you want to START the newly created %s %s? [y/n]: " "${P_KIND}" "${P_NAME}"
                    read RAW_START < /dev/tty
                    CONFIRM_START=`echo "${RAW_START}" | tr -d '\015 \t '`
                    case "${CONFIRM_START}" in
                        [yY])
                            echo "Starting ${P_KIND} ${P_NAME}..."
                            run_ogg_cmd STDOUT "start ${P_KIND} ${P_NAME}" "info ${P_KIND} ${P_NAME}"
                            ;;
                        *)
                            echo "Process ${P_NAME} remains stopped."
                            ;;
                    esac
                    ;;
                *) echo "Aborted by user. No changes were made." ;;
            esac
            echo ""
            
            echo ">>> Current Processes Status:"
            run_ogg_cmd "${WORKDIR}/list_final.log" "info all"
            grep -E "^REPLICAT |^EXTRACT " "${WORKDIR}/list_final.log" | sed "s/^/    /"
            echo ""
            ;;
        8)
            # ==================================================================
            # DISTRIBUTION PATHS MANAGEMENT (19c AdminClient Syntax Compliant)
            # ==================================================================
            while true; do
                echo "--------------------------------------------------------------------------"
                echo " DISTRIBUTION PATHS MANAGEMENT [Deployment: ${DEPLOYMENT}]"
                echo "  1] List Distribution Paths (Summary & Detail)"
                echo "  2] Start Distribution Path(s)"
                echo "  3] Stop Distribution Path(s)"
                echo "  4] Drop Distribution Path(s) [Saves Detail Backup First]"
                echo "  5] Drop and Recreate Distribution Path(s) [Auto-Extract & Restore]"
                echo "  6] Create a New Distribution Path Manually"
                echo "  7] Alter Distribution Path to BEGIN NOW"
                echo "  8] Return to Main Menu"
                echo "--------------------------------------------------------------------------"
                printf "Select an option [1-8]: "
                read PATH_OPT < /dev/tty
                PATH_OPT=`echo "${PATH_OPT}" | tr -d '\015 \t '`
                echo ""

                case "${PATH_OPT}" in
                    1)
                        echo ">>> Fetching Distribution Paths (info distpath all)..."
                        run_ogg_cmd "${WORKDIR}/path_list_raw.log" "info distpath all"
                        cat "${WORKDIR}/path_list_raw.log"
                        awk -f "${WORKDIR}/parse_path_list.awk" "${WORKDIR}/path_list_raw.log" | awk '!seen[$0]++' > "${WORKDIR}/avail_paths.txt"
                        echo ""
                        printf "View detailed output for each discovered path? [y/n]: "
                        read RAW_PDET < /dev/tty
                        CONFIRM_PDET=`echo "${RAW_PDET}" | tr -d '\015 \t '`
                        case "${CONFIRM_PDET}" in
                            [yY])
                                if [ -s "${WORKDIR}/avail_paths.txt" ]; then
                                    while read _PNAME; do
                                        [ -z "${_PNAME}" ] && continue
                                        echo "=== Detail for Path: ${_PNAME} ==="
                                        run_ogg_cmd STDOUT "info distpath ${_PNAME} detail"
                                    done < "${WORKDIR}/avail_paths.txt"
                                else
                                    printf "Enter specific Path Name for detail: "
                                    read SPEC_PNAME < /dev/tty
                                    SPEC_PNAME=`echo "${SPEC_PNAME}" | tr -d '\015 \t '`
                                    [ -n "${SPEC_PNAME}" ] && run_ogg_cmd STDOUT "info distpath ${SPEC_PNAME} detail"
                                fi
                                ;;
                        esac
                        echo ""
                        ;;
                    2)
                        run_ogg_cmd "${WORKDIR}/path_list_raw.log" "info distpath all"
                        awk -f "${WORKDIR}/parse_path_list.awk" "${WORKDIR}/path_list_raw.log" | awk '!seen[$0]++' > "${WORKDIR}/avail_paths.txt"
                        echo ">>> Available Distribution Paths:"
                        sed "s/^/    /" "${WORKDIR}/avail_paths.txt"
                        echo ""
                        printf "Enter Path Name [or ALL]: "
                        read P_PATH < /dev/tty
                        P_PATH=`echo "${P_PATH}" | tr -d '\015 \t '`
                        if [ "${P_PATH}" = "ALL" ] || [ "${P_PATH}" = "all" ] || [ "${P_PATH}" = "*" ]; then
                            while read _PNAME; do
                                [ -z "${_PNAME}" ] && continue
                                echo "Starting DISTPATH ${_PNAME}..."
                                run_ogg_cmd STDOUT "start distpath ${_PNAME}" "info distpath ${_PNAME}"
                            done < "${WORKDIR}/avail_paths.txt"
                        else
                            echo "Starting DISTPATH ${P_PATH}..."
                            run_ogg_cmd STDOUT "start distpath ${P_PATH}" "info distpath ${P_PATH}"
                        fi
                        echo ""
                        ;;
                    3)
                        run_ogg_cmd "${WORKDIR}/path_list_raw.log" "info distpath all"
                        awk -f "${WORKDIR}/parse_path_list.awk" "${WORKDIR}/path_list_raw.log" | awk '!seen[$0]++' > "${WORKDIR}/avail_paths.txt"
                        echo ">>> Available Distribution Paths:"
                        sed "s/^/    /" "${WORKDIR}/avail_paths.txt"
                        echo ""
                        printf "Enter Path Name [or ALL]: "
                        read P_PATH < /dev/tty
                        P_PATH=`echo "${P_PATH}" | tr -d '\015 \t '`
                        if [ "${P_PATH}" = "ALL" ] || [ "${P_PATH}" = "all" ] || [ "${P_PATH}" = "*" ]; then
                            while read _PNAME; do
                                [ -z "${_PNAME}" ] && continue
                                echo "Stopping DISTPATH ${_PNAME}..."
                                run_ogg_cmd STDOUT "stop distpath ${_PNAME}" "info distpath ${_PNAME}"
                            done < "${WORKDIR}/avail_paths.txt"
                        else
                            echo "Stopping DISTPATH ${P_PATH}..."
                            run_ogg_cmd STDOUT "stop distpath ${P_PATH}" "info distpath ${P_PATH}"
                        fi
                        echo ""
                        ;;
                    4|5)
                        echo ">>> Discovering existing Distribution Paths..."
                        run_ogg_cmd "${WORKDIR}/path_list_raw.log" "info distpath all"
                        awk -f "${WORKDIR}/parse_path_list.awk" "${WORKDIR}/path_list_raw.log" | awk '!seen[$0]++' > "${WORKDIR}/avail_paths.txt"

                        echo ">>> Available Distribution Paths:"
                        if [ -s "${WORKDIR}/avail_paths.txt" ]; then
                            sed "s/^/    /" "${WORKDIR}/avail_paths.txt"
                        else
                            echo "    [None auto-parsed from summary - displaying raw summary below]"
                            cat "${WORKDIR}/path_list_raw.log"
                        fi
                        echo ""

                        printf "Enter Path Name [e.g. DP01], Wildcard [e.g. DP*], Comma-Separated [e.g. DP1,DP2], or ALL: "
                        read RAW_PTGT < /dev/tty
                        echo "${RAW_PTGT}" | tr -d '\015\t ' | tr ',' '\n' > "${WORKDIR}/_ptgt_patterns.tmp"
                        cat /dev/null > "${WORKDIR}/path_target_list.txt"

                        while read _PPAT; do
                            [ -z "${_PPAT}" ] && continue
                            _PMATCH=0
                            if [ "${_PPAT}" = "ALL" ] || [ "${_PPAT}" = "all" ] || [ "${_PPAT}" = "*" ]; then
                                cat "${WORKDIR}/avail_paths.txt" >> "${WORKDIR}/path_target_list.txt"
                                _PMATCH=1
                            else
                                while read _AV_PATH; do
                                    case "${_AV_PATH}" in
                                        ${_PPAT})
                                            echo "${_AV_PATH}" >> "${WORKDIR}/path_target_list.txt"
                                            _PMATCH=1
                                            ;;
                                    esac
                                done < "${WORKDIR}/avail_paths.txt"

                                case "${_PPAT}" in
                                    *\**) ;;
                                    *)
                                        if [ ${_PMATCH} -eq 0 ]; then
                                            echo "WARNING: Path [${_PPAT}] not found in active list."
                                            printf "Include [${_PPAT}] anyway (using saved backup or manual entry)? [y/n]: "
                                            read RAW_INC < /dev/tty
                                            CONFIRM_INC=`echo "${RAW_INC}" | tr -d '\015 \t '`
                                            case "${CONFIRM_INC}" in
                                                [yY]) echo "${_PPAT}" >> "${WORKDIR}/path_target_list.txt" ;;
                                            esac
                                        fi
                                        ;;
                                esac
                            fi
                        done < "${WORKDIR}/_ptgt_patterns.tmp"

                        awk '!seen[$0]++' "${WORKDIR}/path_target_list.txt" > "${WORKDIR}/_ptmp.txt"
                        cat "${WORKDIR}/_ptmp.txt" > "${WORKDIR}/path_target_list.txt"

                        if [ ! -s "${WORKDIR}/path_target_list.txt" ]; then
                            echo "WARNING: No Distribution Paths selected."
                            continue
                        fi

                        while read PATH_NAME; do
                            echo "--------------------------------------------------------------------------"
                            PATH_BKP_FILE="${PATH_BACKUP_DIR}/${PATH_NAME}_detail.txt"
                            PATH_META_FILE="${PATH_BACKUP_DIR}/${PATH_NAME}_meta.cfg"

                            echo "--> Capturing fine details for Distribution Path [${PATH_NAME}]..."
                            run_ogg_cmd "${WORKDIR}/_cur_path_det.log" "info distpath ${PATH_NAME} detail"

                            grep -E -i "^ERROR:|does not exist" "${WORKDIR}/_cur_path_det.log" > /dev/null
                            if [ $? -ne 0 ]; then
                                cp "${WORKDIR}/_cur_path_det.log" "${PATH_BKP_FILE}"
                                echo "    [OK] Detail snapshot saved to ${PATH_BKP_FILE}"
                            else
                                echo "    [WARN] Path ${PATH_NAME} not active in OGG. Checking for prior backup..."
                            fi

                            P_SRC_URI=""
                            P_DST_URI=""
                            P_AUTH=""
                            P_ENC=""

                            if [ -s "${PATH_BKP_FILE}" ]; then
                                awk -f "${WORKDIR}/get_path_src.awk" "${PATH_BKP_FILE}" > "${WORKDIR}/_psrc.tmp"
                                read P_SRC_URI < "${WORKDIR}/_psrc.tmp"

                                awk -f "${WORKDIR}/get_path_dst.awk" "${PATH_BKP_FILE}" > "${WORKDIR}/_pdst.tmp"
                                read P_DST_URI < "${WORKDIR}/_pdst.tmp"

                                awk -f "${WORKDIR}/get_path_auth.awk" "${PATH_BKP_FILE}" > "${WORKDIR}/_pauth.tmp"
                                read P_AUTH < "${WORKDIR}/_pauth.tmp"

                                awk -f "${WORKDIR}/get_path_enc.awk" "${PATH_BKP_FILE}" > "${WORKDIR}/_penc.tmp"
                                read P_ENC < "${WORKDIR}/_penc.tmp"

                                echo "PATH_NAME=\"${PATH_NAME}\"" > "${PATH_META_FILE}"
                                echo "P_SRC_URI=\"${P_SRC_URI}\"" >> "${PATH_META_FILE}"
                                echo "P_DST_URI=\"${P_DST_URI}\"" >> "${PATH_META_FILE}"
                                echo "P_AUTH=\"${P_AUTH}\"" >> "${PATH_META_FILE}"
                                echo "P_ENC=\"${P_ENC}\"" >> "${PATH_META_FILE}"
                            fi

                            echo ""
                            echo ">>> Extracted / Saved Path Configuration for [${PATH_NAME}]:"
                            echo "    Source URI          : ${P_SRC_URI:-<Not Detected>}"
                            echo "    Target URI          : ${P_DST_URI:-<Not Detected>}"
                            echo "    Target Auth Method  : ${P_AUTH:-<None / Embedded in URI>}"
                            echo "    Encryption Profile  : ${P_ENC:-<Default / None>}"
                            echo ""

                            if [ "${PATH_OPT}" = "4" ]; then
                                echo ">>> PREVIEW: Planned Execution for DROP of DISTPATH ${PATH_NAME}"
                                echo "    Command 1 : stop distpath ${PATH_NAME}"
                                echo "    Command 2 : delete distpath ${PATH_NAME}"
                                echo ""
                                verify_before_drop "distpath" "${PATH_NAME}" "${PATH_BKP_FILE}"
                                if [ $? -eq 0 ]; then
                                    run_ogg_cmd STDOUT "stop distpath ${PATH_NAME}" "delete distpath ${PATH_NAME}"
                                else
                                    echo "Skipped ${PATH_NAME}."
                                fi
                            else
                                # Option 5: Recreate Path
                                printf "Confirm or Enter Source URI [Press Enter to keep: %s]: " "${P_SRC_URI}"
                                read RAW_IN_SRC < /dev/tty
                                IN_SRC=`echo "${RAW_IN_SRC}" | tr -d '\015 \t '`
                                [ -n "${IN_SRC}" ] && P_SRC_URI="${IN_SRC}"

                                printf "Confirm or Enter Target URI [Press Enter to keep: %s]: " "${P_DST_URI}"
                                read RAW_IN_DST < /dev/tty
                                IN_DST=`echo "${RAW_IN_DST}" | tr -d '\015 \t '`
                                [ -n "${IN_DST}" ] && P_DST_URI="${IN_DST}"

                                printf "Confirm or Enter Target Auth Clause (e.g. USERIDALIAS alias DOMAIN dom) [Press Enter to keep: %s]: " "${P_AUTH}"
                                read RAW_IN_AUTH < /dev/tty
                                IN_AUTH=`echo "${RAW_IN_AUTH}" | tr -d '\015'`
                                if [ "${IN_AUTH}" = "NONE" ] || [ "${IN_AUTH}" = "none" ]; then
                                    P_AUTH=""
                                elif [ -n "${IN_AUTH}" ]; then
                                    P_AUTH="${IN_AUTH}"
                                fi

                                ADD_PATH_CMD="add distpath ${PATH_NAME} source ${P_SRC_URI} target ${P_DST_URI}"
                                if [ -n "${P_AUTH}" ]; then
                                    case "${P_AUTH}" in
                                        *[Tt][Aa][Rr][Gg][Ee][Tt]*) ADD_PATH_CMD="${ADD_PATH_CMD} ${P_AUTH}" ;;
                                        *) ADD_PATH_CMD="${ADD_PATH_CMD} target authentication method ${P_AUTH}" ;;
                                    esac
                                fi

                                echo ""
                                echo ">>> PREVIEW: Planned Execution for RECREATE of DISTPATH ${PATH_NAME}"
                                echo "    Command 1 : stop distpath ${PATH_NAME}"
                                echo "    Command 2 : delete distpath ${PATH_NAME}"
                                echo "    Command 3 : ${ADD_PATH_CMD}"
                                echo ""
                                verify_before_drop "distpath" "${PATH_NAME}" "${PATH_BKP_FILE}"
                                if [ $? -eq 0 ]; then
                                    run_ogg_cmd STDOUT \
                                        "stop distpath ${PATH_NAME}" \
                                        "delete distpath ${PATH_NAME}" \
                                        "${ADD_PATH_CMD}" \
                                        "info distpath ${PATH_NAME} detail"
                                    echo ""
                                    printf "Do you want to START DISTPATH %s now? [y/n]: " "${PATH_NAME}"
                                    read RAW_PSTART < /dev/tty
                                    CONFIRM_PSTART=`echo "${RAW_PSTART}" | tr -d '\015 \t '`
                                    case "${CONFIRM_PSTART}" in
                                        [yY]) run_ogg_cmd STDOUT "start distpath ${PATH_NAME}" "info distpath ${PATH_NAME}" ;;
                                    esac
                                else
                                    echo "Skipped ${PATH_NAME}."
                                fi
                            fi
                        done < "${WORKDIR}/path_target_list.txt"
                        echo ""
                        echo ">>> Current Distribution Paths Status:"
                        run_ogg_cmd STDOUT "info distpath all"
                        echo ""
                        ;;
                    6)
                        printf "Enter New Distribution Path Name: "
                        read NEW_PNAME < /dev/tty
                        NEW_PNAME=`echo "${NEW_PNAME}" | tr -d '\015 \t '`

                        printf "Enter Source Trail URI [e.g. trail://host:port/services/v2/sources?trail=tr]: "
                        read NEW_PSRC < /dev/tty
                        NEW_PSRC=`echo "${NEW_PSRC}" | tr -d '\015 \t '`

                        printf "Enter Target URI [e.g. ws://target-host:port/services/v2/targets?trail=tr]: "
                        read NEW_PDST < /dev/tty
                        NEW_PDST=`echo "${NEW_PDST}" | tr -d '\015 \t '`

                        printf "Enter Optional Target Auth Clause [e.g. target authentication method useridalias ALIAS domain DOM] or press Enter: "
                        read NEW_PAUTH < /dev/tty
                        NEW_PAUTH=`echo "${NEW_PAUTH}" | tr -d '\015'`

                        ADD_PATH_CMD="add distpath ${NEW_PNAME} source ${NEW_PSRC} target ${NEW_PDST} ${NEW_PAUTH}"
                        echo ""
                        echo "Preview Command: ${ADD_PATH_CMD}"
                        printf "Create Distribution Path %s now? [y/n]: " "${NEW_PNAME}"
                        read RAW_PCRE < /dev/tty
                        CONFIRM_PCRE=`echo "${RAW_PCRE}" | tr -d '\015 \t '`
                        case "${CONFIRM_PCRE}" in
                            [yY])
                                run_ogg_cmd STDOUT "${ADD_PATH_CMD}" "info distpath ${NEW_PNAME} detail"
                                printf "Do you want to START DISTPATH %s now? [y/n]: " "${NEW_PNAME}"
                                read RAW_PSTART < /dev/tty
                                CONFIRM_PSTART=`echo "${RAW_PSTART}" | tr -d '\015 \t '`
                                case "${CONFIRM_PSTART}" in
                                    [yY]) run_ogg_cmd STDOUT "start distpath ${NEW_PNAME}" "info distpath ${NEW_PNAME}" ;;
                                esac
                                ;;
                        esac
                        echo ""
                        ;;
                    7)
                        run_ogg_cmd STDOUT "info distpath all"
                        printf "Enter Path Name to Alter to BEGIN NOW: "
                        read ALT_PNAME < /dev/tty
                        ALT_PNAME=`echo "${ALT_PNAME}" | tr -d '\015 \t '`
                        run_ogg_cmd STDOUT "stop distpath ${ALT_PNAME}" "alter distpath ${ALT_PNAME} begin now" "info distpath ${ALT_PNAME}"
                        printf "Do you want to START DISTPATH %s now? [y/n]: " "${ALT_PNAME}"
                        read RAW_PSTART < /dev/tty
                        CONFIRM_PSTART=`echo "${RAW_PSTART}" | tr -d '\015 \t '`
                        case "${CONFIRM_PSTART}" in
                            [yY]) run_ogg_cmd STDOUT "start distpath ${ALT_PNAME}" "info distpath ${ALT_PNAME}" ;;
                        esac
                        echo ""
                        ;;
                    8)
                        break
                        ;;
                    *)
                        echo "Invalid selection."
                        ;;
                esac
            done
            ;;
        9)
            # ==================================================================
            # DB CONNECTIONS / CREDENTIAL STORE MANAGEMENT
            # ==================================================================
            while true; do
                echo "--------------------------------------------------------------------------"
                echo " DB CONNECTIONS / CREDENTIAL STORE [Deployment: ${DEPLOYMENT}]"
                echo "  1] List All DB Connections (Domains & Aliases)"
                echo "  2] Test / Verify a DB Connection (dblogin)"
                echo "  3] Create or Update a DB Connection Alias"
                echo "  4] Drop a DB Connection Alias"
                echo "  5] Return to Main Menu"
                echo "--------------------------------------------------------------------------"
                printf "Select an option [1-5]: "
                read DB_OPT < /dev/tty
                DB_OPT=`echo "${DB_OPT}" | tr -d '\015 \t '`
                echo ""

                case "${DB_OPT}" in
                    1)
                        echo ">>> Querying Credential Store..."
                        run_ogg_cmd STDOUT "info credentialstore"
                        printf "Filter by a specific Domain (e.g. OracleGoldenGate)? [Press Enter to skip]: "
                        read FILT_DOM < /dev/tty
                        FILT_DOM=`echo "${FILT_DOM}" | tr -d '\015 \t '`
                        if [ -n "${FILT_DOM}" ]; then
                            run_ogg_cmd STDOUT "info credentialstore domain ${FILT_DOM}"
                        fi
                        echo ""
                        ;;
                    2)
                        printf "Enter Credential Alias to Test [e.g. CDB_TGT01]: "
                        read TST_ALIAS < /dev/tty
                        TST_ALIAS=`echo "${TST_ALIAS}" | tr -d '\015 \t '`
                        printf "Enter Domain [Press Enter for OracleGoldenGate]: "
                        read TST_DOM < /dev/tty
                        TST_DOM=`echo "${TST_DOM}" | tr -d '\015 \t '`
                        [ -z "${TST_DOM}" ] && TST_DOM="OracleGoldenGate"

                        run_ogg_cmd STDOUT "dblogin useridalias ${TST_ALIAS} domain ${TST_DOM}"
                        echo ""
                        ;;
                    3)
                        printf "Enter Credential Alias Name [e.g. PDB_TGT01]: "
                        read ADD_ALIAS < /dev/tty
                        ADD_ALIAS=`echo "${ADD_ALIAS}" | tr -d '\015 \t '`

                        printf "Enter DB Connect String / User [e.g. c##ggadmin@CDB01 or ggadmin@PDB01]: "
                        read ADD_DBUSER < /dev/tty
                        ADD_DBUSER=`echo "${ADD_DBUSER}" | tr -d '\015 \t '`

                        printf "Enter DB Password: "
                        stty -echo
                        read RAW_DBPASS < /dev/tty
                        stty echo
                        echo ""
                        ADD_DBPASS=`echo "${RAW_DBPASS}" | tr -d '\015 \t '`

                        printf "Enter Credential Domain [Press Enter for OracleGoldenGate]: "
                        read ADD_DOM < /dev/tty
                        ADD_DOM=`echo "${ADD_DOM}" | tr -d '\015 \t '`
                        [ -z "${ADD_DOM}" ] && ADD_DOM="OracleGoldenGate"

                        echo ""
                        echo ">>> PREVIEW: alter credentialstore add user ${ADD_DBUSER} password ****** alias ${ADD_ALIAS} domain ${ADD_DOM}"
                        printf "Execute this Credential Store addition? [y/n]: "
                        read RAW_CADD < /dev/tty
                        CONFIRM_CADD=`echo "${RAW_CADD}" | tr -d '\015 \t '`
                        case "${CONFIRM_CADD}" in
                            [yY])
                                run_ogg_cmd STDOUT \
                                    "alter credentialstore add user ${ADD_DBUSER} password ${ADD_DBPASS} alias ${ADD_ALIAS} domain ${ADD_DOM}" \
                                    "info credentialstore domain ${ADD_DOM}"
                                ;;
                        esac
                        echo ""
                        ;;
                    4)
                        run_ogg_cmd STDOUT "info credentialstore"
                        printf "Enter Credential Alias to DROP: "
                        read DEL_ALIAS < /dev/tty
                        DEL_ALIAS=`echo "${DEL_ALIAS}" | tr -d '\015 \t '`

                        printf "Enter Credential Domain [Press Enter for OracleGoldenGate]: "
                        read DEL_DOM < /dev/tty
                        DEL_DOM=`echo "${DEL_DOM}" | tr -d '\015 \t '`
                        [ -z "${DEL_DOM}" ] && DEL_DOM="OracleGoldenGate"

                        echo ""
                        echo ">>> PREVIEW: alter credentialstore delete user alias ${DEL_ALIAS} domain ${DEL_DOM}"
                        printf "Are you sure you want to DROP credential alias %s? [y/n]: " "${DEL_ALIAS}"
                        read RAW_CDEL < /dev/tty
                        CONFIRM_CDEL=`echo "${RAW_CDEL}" | tr -d '\015 \t '`
                        case "${CONFIRM_CDEL}" in
                            [yY])
                                run_ogg_cmd STDOUT \
                                    "alter credentialstore delete user alias ${DEL_ALIAS} domain ${DEL_DOM}" \
                                    "info credentialstore domain ${DEL_DOM}"
                                ;;
                        esac
                        echo ""
                        ;;
                    5)
                        break
                        ;;
                    *)
                        echo "Invalid selection."
                        ;;
                esac
            done
            ;;
        10)
            # ==================================================================
            # TRAIL PURGE TASKS MANAGEMENT (19c CLI + Native AIX Perl REST)
            # ==================================================================
            while true; do
                echo "--------------------------------------------------------------------------"
                echo " TRAIL PURGE TASKS (PurgeExtTrails) [Deployment: ${DEPLOYMENT}]"
                echo "  1] List Existing PurgeExtTrails Rules (CLI & Native REST)"
                echo "  2] Add a New PurgeExtTrails Rule"
                echo "  3] Edit / Alter an Existing PurgeExtTrails Rule"
                echo "  4] Delete a PurgeExtTrails Rule"
                echo "  5] View PurgeExtTrails Status & Full REST JSON Details"
                echo "  6] Return to Main Menu"
                echo "--------------------------------------------------------------------------"
                printf "Select an option [1-6]: "
                read TSK_OPT < /dev/tty
                TSK_OPT=`echo "${TSK_OPT}" | tr -d '\015 \t '`
                echo ""

                case "${TSK_OPT}" in
                    1)
                        echo ">>> Querying Tasks via 19c AdminClient (info all tasks)..."
                        run_ogg_cmd STDOUT "info all tasks"
                        echo ""
                        echo ">>> Querying 23c PurgeExtTrails Rules via Native AIX REST Client..."
                        run_ogg_rest "GET" "/services/${DEPLOYMENT}/adminsrvr/v2/tasks/PurgeExtTrails"
                        echo ""
                        ;;
                    2)
                        printf "Use Checkpoints (usecheckpoints)? [y/n, Default: y]: "
                        read RAW_UCHK < /dev/tty
                        T_UCHK=`echo "${RAW_UCHK}" | tr -d '\015 \t '`
                        case "${T_UCHK}" in
                            [nN]) UCHK_CLAUSE="nousecheckpoints" ;;
                            *)    UCHK_CLAUSE="usecheckpoints" ;;
                        esac

                        printf "Enter Keep Rule (e.g. hours 48, days 7, files 5) [Default: hours 48]: "
                        read RAW_KEEP < /dev/tty
                        T_KEEP=`echo "${RAW_KEEP}" | tr -d '\015'`
                        [ -z "${T_KEEP}" ] && T_KEEP="hours 48"

                        printf "Enter Purge Frequency (e.g. minutes 60, hours 2) [Default: hours 1]: "
                        read RAW_FREQ < /dev/tty
                        T_FREQ=`echo "${RAW_FREQ}" | tr -d '\015'`
                        [ -z "${T_FREQ}" ] && T_FREQ="hours 1"

                        printf "Enter Optional Trail Clause (e.g. trail tr) or press Enter for all trails: "
                        read RAW_TTRAIL < /dev/tty
                        T_TRAIL=`echo "${RAW_TTRAIL}" | tr -d '\015'`

                        ADD_TSK_CMD="add PurgeExtTrails ${T_TRAIL} ${UCHK_CLAUSE} min ${T_KEEP} frequency ${T_FREQ}"
                        echo ""
                        echo ">>> PREVIEW: ${ADD_TSK_CMD}"
                        printf "Execute this PurgeExtTrails command now? [y/n]: "
                        read RAW_TADD < /dev/tty
                        CONFIRM_TADD=`echo "${RAW_TADD}" | tr -d '\015 \t '`
                        case "${CONFIRM_TADD}" in
                            [yY])
                                run_ogg_cmd STDOUT "${ADD_TSK_CMD}" "info all tasks"
                                ;;
                        esac
                        echo ""
                        ;;
                    3)
                        run_ogg_rest "GET" "/services/${DEPLOYMENT}/adminsrvr/v2/tasks/PurgeExtTrails"
                        printf "Use Checkpoints (usecheckpoints)? [y/n, Default: y]: "
                        read RAW_EUCHK < /dev/tty
                        E_UCHK=`echo "${RAW_EUCHK}" | tr -d '\015 \t '`
                        case "${E_UCHK}" in
                            [nN]) EUCHK_CLAUSE="nousecheckpoints" ;;
                            *)    EUCHK_CLAUSE="usecheckpoints" ;;
                        esac

                        printf "Enter New Keep Rule (e.g. hours 72, days 3, files 2): "
                        read RAW_EKEEP < /dev/tty
                        E_KEEP=`echo "${RAW_EKEEP}" | tr -d '\015'`

                        printf "Enter New Frequency (e.g. hours 1, minutes 30): "
                        read RAW_EFREQ < /dev/tty
                        E_FREQ=`echo "${RAW_EFREQ}" | tr -d '\015'`

                        printf "Enter Optional Trail Clause (e.g. trail tr) or press Enter: "
                        read RAW_ETRAIL < /dev/tty
                        E_TRAIL=`echo "${RAW_ETRAIL}" | tr -d '\015'`

                        ALT_TSK_CMD="alter PurgeExtTrails ${E_TRAIL} ${EUCHK_CLAUSE} min ${E_KEEP} frequency ${E_FREQ}"
                        echo ""
                        echo ">>> PREVIEW: ${ALT_TSK_CMD}"
                        printf "Alter PurgeExtTrails now? [y/n]: "
                        read RAW_TALT < /dev/tty
                        CONFIRM_TALT=`echo "${RAW_TALT}" | tr -d '\015 \t '`
                        case "${CONFIRM_TALT}" in
                            [yY])
                                run_ogg_cmd STDOUT "${ALT_TSK_CMD}" "info all tasks"
                                ;;
                        esac
                        echo ""
                        ;;
                    4)
                        run_ogg_rest "GET" "/services/${DEPLOYMENT}/adminsrvr/v2/tasks/PurgeExtTrails"
                        printf "Enter Optional Trail Parameter to Delete (or press Enter to delete global PurgeExtTrails): "
                        read RAW_DTRAIL < /dev/tty
                        D_TRAIL=`echo "${RAW_DTRAIL}" | tr -d '\015'`

                        echo ""
                        echo ">>> PREVIEW: delete PurgeExtTrails ${D_TRAIL}"
                        printf "Are you sure you want to delete this PurgeExtTrails rule? [y/n]: "
                        read RAW_TDEL < /dev/tty
                        CONFIRM_TDEL=`echo "${RAW_TDEL}" | tr -d '\015 \t '`
                        case "${CONFIRM_TDEL}" in
                            [yY])
                                run_ogg_cmd STDOUT "delete PurgeExtTrails ${D_TRAIL}" "info all tasks"
                                ;;
                        esac
                        echo ""
                        ;;
                    5)
                        echo ">>> 1. Querying Tasks via AdminClient (info all tasks)..."
                        run_ogg_cmd STDOUT "info all tasks"
                        echo ""
                        echo ">>> 2. Querying 23c REST API (/tasks/PurgeExtTrails) via Native AIX Perl..."
                        run_ogg_rest "GET" "/services/${DEPLOYMENT}/adminsrvr/v2/tasks/PurgeExtTrails"
                        echo ""
                        echo ">>> 3. Querying 23c REST API (/config/files/PurgeExtTrails) via Native AIX Perl..."
                        run_ogg_rest "GET" "/services/${DEPLOYMENT}/adminsrvr/v2/config/files/PurgeExtTrails"
                        echo ""
                        ;;
                    6)
                        break
                        ;;
                    *)
                        echo "Invalid selection."
                        ;;
                esac
            done
            ;;
        11)
            # ==================================================================
            # SWITCH DEPLOYMENT
            # ==================================================================
            if [ -s "${WORKDIR}/dep_list.txt" ]; then
                echo ">>> Available Deployments:"
                IDX=1
                while read DEP_NAME; do
                    echo "  ${IDX}] ${DEP_NAME}"
                    IDX=`expr $IDX + 1`
                done < "${WORKDIR}/dep_list.txt"
                echo ""
                MAX_SEL=`expr $IDX - 1`
                printf "Select Deployment [1-%s]: " "${MAX_SEL}"
                read DEP_SEL < /dev/tty
                DEP_SEL=`echo "${DEP_SEL}" | tr -d '\015 \t '`
                SELECTED_DEP=`awk -v sel="${DEP_SEL}" 'NR==sel {print $1}' "${WORKDIR}/dep_list.txt"`
                if [ -n "${SELECTED_DEP}" ]; then
                    DEPLOYMENT="${SELECTED_DEP}"
                    echo "Switched active deployment to: ${DEPLOYMENT}"
                else
                    echo "Invalid selection. Keeping deployment: ${DEPLOYMENT}"
                fi
            else
                printf "Enter Deployment Name Manually: "
                read RAW_DEP < /dev/tty
                TMP_DEP=`echo "${RAW_DEP}" | tr -d '\015 \t '`
                [ -n "${TMP_DEP}" ] && DEPLOYMENT="${TMP_DEP}"
            fi
            echo ""
            ;;
        12)
            echo "Exiting OGG Pipeline Manager..."
            exit 0
            ;;
        *)
            echo "Invalid option. Please select a number between 1 and 12."
            ;;
    esac
done