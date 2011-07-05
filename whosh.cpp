#include <cstdlib>
#include <getopt.h>
#include <libpq-fe.h>
#include <sstream>
#include <iostream>
#include <iomanip>
#include <string.h>

#define OSMIUM_MAIN
#include <osmium.hpp>

class pgCopyHandler : public Osmium::Handler::Base {

PGconn *node_conn, *way_conn, *rel_conn, *relmem_conn, *waynode_conn, *user_conn;
static const char d = ';';
long int node_count, way_count, rel_count, relmem_count, waynode_count, user_count;
std::vector<bool> user;


public:

    pgCopyHandler(std::string dbConnectionString) {
        node_count = 0;
        way_count = 0;
        rel_count = 0;
        relmem_count = 0;
        waynode_count = 0;
        user_count = 0;

        way_conn = PQconnectdb(dbConnectionString.c_str());
        if(PQstatus(way_conn) != CONNECTION_OK) {
            std::cerr << "DB Connection for Ways failed: ";
            std::cerr << PQerrorMessage(way_conn) << std::endl;
            exit(1);
        }

        rel_conn = PQconnectdb(dbConnectionString.c_str());
        if(PQstatus(rel_conn) != CONNECTION_OK) {
            std::cerr << "DB Connection for Relations failed: ";
            std::cerr << PQerrorMessage(rel_conn) << std::endl;
            exit(1);
        }

        relmem_conn = PQconnectdb(dbConnectionString.c_str());
        if(PQstatus(relmem_conn) != CONNECTION_OK) {
            std::cerr << "DB Connection for Relation Members failed: ";
            std::cerr << PQerrorMessage(relmem_conn) << std::endl;
            exit(1);
        }
/*
        user_conn = PQconnectdb(dbConnectionString.c_str());
        if(PQstatus(user_conn) != CONNECTION_OK) {
            std::cerr << "DB Connection for Users failed: ";
            std::cerr << PQerrorMessage(user_conn) << std::endl;
            exit(1);
        }
*/
        waynode_conn = PQconnectdb(dbConnectionString.c_str());
        if(PQstatus(waynode_conn) != CONNECTION_OK) {
            std::cerr << "DB Connection for Relation Members failed: ";
            std::cerr << PQerrorMessage(waynode_conn) << std::endl;
            exit(1);
        }

    }

    ~pgCopyHandler() {
    }

    void printStats() {
        std::cerr << '\r';
        std::cerr << "Nodes: " << node_count << " Ways: " << way_count << " Relations: " << rel_count << " Relation Members: " << relmem_count << " Way Nodes: " << waynode_count;
    }

    std::string escape(std::string str) {
        std::ostringstream ret;
        size_t i;
        for(i=0;i<str.length();++i) {
            if (str[i] == d) {
                ret << "\\";
                ret << d;
            }
            else if (str[i] == '\"') {
                ret << "\\\\\"";
            }
            else if (str[i] == '\r') {
                ret << "\\r";
            }
            else if (str[i] == '\n') {
                ret << "\\n";
            }
            else if (str[i] == '\\') {
                ret << "\\\\\\\\";
            }
            else {
                ret << str[i];
            }
        }
        return ret.str();
    }

    std::string tagsToHstore(Osmium::OSM::Object *obj) {
        if(obj->tag_count() != 0) {
            std::ostringstream out;
            out << "\"" << escape(obj->get_tag_key(0)) << "\"=>\"" << escape(obj->get_tag_value(0)) << "\"";
            for(int i=1;i<obj->tag_count();++i) {
                out << ",\"" << escape(obj->get_tag_key(i)) << "\"=>\"" << escape(obj->get_tag_value(i)) << "\"";
            }
            return out.str();
        } else {
            std::string str("");
            return str;
        }
    }

    std::string genNodesArray(Osmium::OSM::Way *way) {
        osm_sequence_id_t nodecount = way->node_count();
        osm_sequence_id_t i;
        std::ostringstream out;
        if (nodecount > 0) {
            out << "{" << way->get_node_id(0);
            for(i=1;i<nodecount;++i) {
                out << "," << way->get_node_id(i);
            }
            out << "}";
        }
        else {
            out << "{}";
        }
        return out.str();
    }
/*
    void addUser(Osmium::OSM::Object *obj) {
        if (obj->get_uid() >= 0) {
            if (user.size() < obj->get_uid()) {
                user.resize(obj->get_uid()+1);
            }
            if (!user[obj->get_uid()]) {
                std::ostringstream user_str;
                user_str << obj->get_uid() << d;
                user_str << escape(obj->get_user());
                user_str << std::endl;

                int success = PQputCopyData(user_conn, user_str.str().c_str(), user_str.str().length());
                if (success == 1) {
                    user_count++;
                    if (user_count % 1000 == 0) {
                        printStats();
                    }
                }
                else {
                    std::cerr << "Meh on User: " << user_str.str() << std::endl;
                }
                user[obj->get_uid()] = true;
            }
        }
    }
*/
    void sendBegin(PGconn *conn) {
        PGresult *res;
        res = PQexec(conn, "BEGIN;");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            std::cerr << "COMMAND BEGIN failded: ";
            std::cerr << PQerrorMessage(conn) << std::endl;
            PQclear(res);
            PQfinish(conn);
            exit(1);
        }
    }
    
    void finishHim(PGconn *conn) {
        PGresult *res;
        if(PQputCopyEnd(conn, NULL)) {
            std::cerr << std::endl << "Copy End..." << std::endl;
        }
        while((res = PQgetResult(conn)) != NULL) {
            std::cerr << "\r" << "Waiting for Copy to finish";
        }
        std::cerr << "\r" << "Copy finished             " << std:: endl;
        PQendcopy(conn);
        while((res = PQgetResult(conn)) != NULL) {
            std::cerr << "\r" << "Waiting for Server to Sync";
        }
        std::cerr << "Sync Done" << std:: endl;

        res = PQexec(conn, "END;");
        if (PQresultStatus(res) != PGRES_COMMAND_OK) {
            std::cerr << "COMMAND END failded: ";
            std::cerr << PQerrorMessage(conn) << std::endl;
            PQclear(res);
            PQfinish(conn);
            return;
        }
        PQfinish(conn);
    }

    void callback_init() {
        PGresult *res;
        sendBegin(way_conn);
        sendBegin(rel_conn);
        sendBegin(relmem_conn);
        //sendBegin(user_conn);
        sendBegin(waynode_conn);

       res = PQexec(way_conn, "COPY ways (id, version, user_id, tstamp, changeset_id, tags, nodes, linestring) FROM STDIN DELIMITER ';'");
        if (PQresultStatus(res) != PGRES_COPY_IN) {
            std::cerr << "COMMAND COPY failded: ";
            std::cerr << PQerrorMessage(way_conn) << std::endl;
            PQclear(res);
            PQfinish(node_conn);
            PQfinish(way_conn);
            PQfinish(rel_conn);
            PQfinish(relmem_conn);
            //PQfinish(user_conn);
            PQfinish(waynode_conn);
            exit(1);
        }

       res = PQexec(rel_conn, "COPY relations (id, version, user_id, tstamp, changeset_id, tags) FROM STDIN DELIMITER ';'");
        if (PQresultStatus(res) != PGRES_COPY_IN) {
            std::cerr << "COMMAND COPY failded: ";
            std::cerr << PQerrorMessage(rel_conn) << std::endl;
            PQclear(res);
            PQfinish(node_conn);
            PQfinish(way_conn);
            PQfinish(rel_conn);
            PQfinish(relmem_conn);
            //PQfinish(user_conn);
            PQfinish(waynode_conn);
            exit(1);
        }

       res = PQexec(relmem_conn, "COPY relation_members (relation_id, member_id, member_type, member_role, sequence_id) FROM STDIN DELIMITER ';'");
        if (PQresultStatus(res) != PGRES_COPY_IN) {
            std::cerr << "COMMAND COPY failded: ";
            std::cerr << PQerrorMessage(relmem_conn) << std::endl;
            PQclear(res);
            PQfinish(node_conn);
            PQfinish(way_conn);
            PQfinish(rel_conn);
            PQfinish(relmem_conn);
            //PQfinish(user_conn);
            PQfinish(waynode_conn);
            exit(1);
        }
/*
        res = PQexec(user_conn, "COPY users (id, name) FROM STDIN DELIMITER ';'");
        if (PQresultStatus(res) != PGRES_COPY_IN) {
            std::cerr << "COMMAND COPY failded: ";
            std::cerr << PQerrorMessage(user_conn) << std::endl;
            PQclear(res);
            PQfinish(node_conn);
            PQfinish(way_conn);
            PQfinish(rel_conn);
            PQfinish(relmem_conn);
            //PQfinish(user_conn);
            PQfinish(waynode_conn);
            exit(1);
        }
*/
       res = PQexec(waynode_conn, "COPY way_nodes (way_id, node_id, sequence_id) FROM STDIN DELIMITER ';'");
        if (PQresultStatus(res) != PGRES_COPY_IN) {
            std::cerr << "COMMAND COPY failded: ";
            std::cerr << PQerrorMessage(waynode_conn) << std::endl;
            PQclear(res);
            PQfinish(node_conn);
            PQfinish(way_conn);
            PQfinish(rel_conn);
            PQfinish(relmem_conn);
            //PQfinish(user_conn);
            PQfinish(waynode_conn);
            exit(1);
        }
    }

    void callback_before_nodes() {
        node_conn = PQconnectdb(dbConnectionString.c_str());
        if(PQstatus(node_conn) != CONNECTION_OK) {
            std::cerr << "DB Connection for Nodes failed: ";
            std::cerr << PQerrorMessage(node_conn) << std::endl;
            exit(1);
        }

        PGresult *res;
        sendBegin(node_conn);
        res = PQexec(node_conn, "COPY nodes (id, version, user_id, tstamp, changeset_id, tags, geom) FROM STDIN DELIMITER ';'");
        if (PQresultStatus(res) != PGRES_COPY_IN) {
            std::cerr << "COMMAND COPY for nodes failed: ";
            std::cerr << PQerrorMessage(node_conn) << std::endl;
            PQclear(res);
            PQfinish(node_conn);
            exit(1);
        }
    }

    void callback_node(Osmium::OSM::Node *node) {
        std::ostringstream node_str;
        node_str << node->get_id() << d;
        node_str << node->get_version() << d;
        node_str << node->get_uid() << d;
        node_str << node->get_timestamp_as_string().replace(10,1," ").erase(19,1) << d;
        node_str << node->get_changeset() << d;
        node_str << tagsToHstore(node) << d;
        node_str << "SRID=4326\\;POINT(" << node->get_lat() << " " << node->get_lon() << ")";
        node_str << std::endl;

        int success = PQputCopyData(node_conn, node_str.str().c_str(), node_str.str().length());
        if (success == 1) {
            node_count++;
            if (node_count % 10000 == 0) {
                printStats();
            }
        }
        else {
            std::cerr << "Meh on Node: " << node_str.str() << std::endl;
        }

        //addUser(node);
        if (user_count < node->get_uid()) {
            user_count = node->get_uid();
        }
    }

    void callback_after_nodes() {
        printStats()
        finishHim(node_conn);
    }

    void callback_way(Osmium::OSM::Way *way) {
        std::ostringstream way_str;
        way_str << way->get_id() << d;
        way_str << way->get_version() << d;
        way_str << way->get_uid() << d;
        way_str << way->get_timestamp_as_string().replace(10,1," ").erase(19,1) << d;
        way_str << way->get_changeset() << d;
        way_str << tagsToHstore(way) << d;
        way_str << genNodesArray(way) << d;
        way_str << "\\N";
        way_str << std::endl;

        int success = PQputCopyData(way_conn, way_str.str().c_str(), way_str.str().length());
        if (success == 1) {
            way_count++;
            if (way_count % 10000 == 0) {
                printStats();
            }
        }
        else {
            std::cerr << "Meh on Way: " << way_str.str() << std::endl;
        }

        osm_sequence_id_t nodecount = way->node_count();
        osm_sequence_id_t i;
        std::ostringstream waynode_str;
        if (nodecount > 0) {
            for(i=0;i<nodecount;++i) {
                waynode_str << way->get_id() << d;
                waynode_str << way->get_node_id(i) << d;
                waynode_str << i << std::endl;
            }
        }
        success = PQputCopyData(waynode_conn, waynode_str.str().c_str(), waynode_str.str().length());
        if (success == 1) {
            waynode_count+=nodecount;
            if (waynode_count % 10000 == 0) {
                printStats();
            }
        }
        else {
            std::cerr << "Meh on Waynode: " << way_str.str() << std::endl;
        }

        //addUser(way);
        if (user_count < way->get_uid()) {
            user_count = way->get_uid();
        }

    }

    void callback_relation(Osmium::OSM::Relation *rel) {
        std::ostringstream rel_str;
        rel_str << rel->get_id() << d;
        rel_str << rel->get_version() << d;
        rel_str << rel->get_uid() << d;
        rel_str << rel->get_timestamp_as_string().replace(10,1," ").erase(19,1) << d;
        rel_str << rel->get_changeset() << d;
        rel_str << tagsToHstore(rel);
        rel_str << std::endl;

        int success = PQputCopyData(rel_conn, rel_str.str().c_str(), rel_str.str().length());
        if (success == 1) {
            rel_count++;
            if (rel_count % 10000 == 0) {
                printStats();
            }
        }
        else {
            std::cerr << "Meh on Rel: " << rel_str.str() << std::endl;
        }

        Osmium::OSM::RelationMemberList members = rel->members();
        osm_sequence_id_t membercount = members.size();
        osm_sequence_id_t i;
        std::ostringstream relmem_str;
        for(i=0;i<membercount;++i) {
            const Osmium::OSM::RelationMember* member = rel->get_member(i);

            relmem_str << rel->get_id() << d;
            relmem_str << member->get_ref() << d;
            relmem_str << member->get_type() << d;
            relmem_str << escape(member->get_role()) << d;
            relmem_str << i;
            relmem_str << std::endl;
        }
        
        success = PQputCopyData(relmem_conn, relmem_str.str().c_str(), relmem_str.str().length());
        if (success == 1) {
            relmem_count+=membercount;
            if (relmem_count % 10000 == 0) {
                printStats();
            }
        }
        else {
            std::cerr << "Meh on Relmem: " << relmem_str.str() << std::endl;
        }

        //addUser(rel);
        if (user_count < rel->get_uid()) {
            user_count = rel->get_uid();
        }
    }

    void callback_final() {
        printStats()
        std::cerr << std::endl << "Max UserID was: " << user_count << std::endl;
        finishHim(way_conn);
        finishHim(rel_conn);
        finishHim(relmem_conn);
        //finishHim(user_conn);
        finishHim(waynode_conn);
    }

};

void print_help() {
    std::cout << "whosh [OPTIONS] [INFILE]\n\n" \
              << "If INFILE is not given stdin is assumed.\n" \
              << "File format is given as suffix in format .TYPE[.ENCODING].\n" \
              << "\nFile types:\n" \
              << "  osm     normal OSM file\n" \
              << "  osh     OSM file with history information\n" \
              << "\nFile encodings:\n" \
              << "  (none)  XML encoding\n" \
              << "  gz      XML encoding compressed with gzip\n" \
              << "  bz2     XML encoding compressed with bzip2\n" \
              << "  pbf     binary PBF encoding\n" \
              << "\nOptions:\n" \
              << "  -h, --help                This help message\n" \
              << "  -d, --debug               Enable debugging output\n" \
              << "  -c, --host=<dbhost>       DB Host\n" \
              << "  -p, --port=<dbport>       DB port\n" \
              << "  -b, --db=<db>             Database\n" \
              << "  -u, --user=<dbuser>       DB User\n" \
              << "  -w, --pass=<dbpass>       DB Password\n";
}

int main(int argc, char *argv[]) {
    static struct option long_options[] = {
        {"debug",       no_argument, 0, 'd'},
        {"help",        no_argument, 0, 'h'},
        {"host",  required_argument, 0, 'c'},
        {"port",  required_argument, 0, 'p'},
        {"db",    required_argument, 0, 'b'},
        {"user",  required_argument, 0, 'u'},
        {"pass",  required_argument, 0, 'w'},
        {0, 0, 0, 0}
    };

    bool debug = false;

    std::string host="";
    std::string port="";
    std::string db="";
    std::string user="";
    std::string pass="";

    while (true) {
        int c = getopt_long(argc, argv, "dhc:p:b:u:w:", long_options, 0);
        if (c == -1) {
            break;
        }

        switch (c) {
            case 'd':
                debug = true;
                break;
            case 'h':
                print_help();
                exit(0);
            case 'c':
                host = optarg;
                break;
            case 'p':
                port = optarg;
                break;
            case 'b':
                db = optarg;
                break;
            case 'u':
                user = optarg;
                break;
            case 'w':
                pass = optarg;
                break;
            default:
                exit(1);
        }
    }

    int remaining_args = argc - optind;
    if ( (remaining_args != 1) || host.empty() || db.empty() || user.empty() ) {
        std::cerr << "You are doin' it wrong. Use -h to get help." << std::endl;
        exit(1);
    }

    std::string input = argv[optind]; 
    std::string conninfo = "dbname = " + db + " host = " + host + " user = "+ user + " password = " + pass;

    // hide cmd line args
    for (int i=1;i<argc;++i) {
        unsigned int len = strlen(argv[i]);
        for(unsigned int j=0;j<len;++j) {
            argv[i][j]=0;
        }
    }

    Osmium::Framework osmium(debug);
    Osmium::OSMFile infile(input);

    pgCopyHandler handler(conninfo);
    infile.read(handler);
}

