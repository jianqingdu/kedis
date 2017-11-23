/*
 * util.cpp
 *
 *  Created on: 2016-3-14
 *      Author: ziteng
 */

#include "util.h"
#include <assert.h>
#include <math.h>

uint64_t get_tick_count()
{
	struct timeval tval;
	gettimeofday(&tval, NULL);
    
	uint64_t current_tick = tval.tv_sec * 1000L + tval.tv_usec / 1000L;
	return current_tick;
}

bool is_valid_ip(const char *ip)
{
    struct sockaddr_in sa;
    int result = inet_pton(AF_INET, ip, &(sa.sin_addr));
    return result != 0;
}

void write_pid(const string& name)
{
	uint32_t current_pid = (uint32_t) getpid();
    FILE* f = fopen(name.c_str(), "w");
    if (f) {
        char szPid[32];
        snprintf(szPid, sizeof(szPid), "%d", current_pid);
        fwrite(szPid, strlen(szPid), 1, f);
        fclose(f);
    }
}

int get_file_content(const char* filename, string& content)
{
    struct stat st_buf;
    if (stat(filename, &st_buf) != 0) {
        fprintf(stderr, "stat failed\n");
        return 1;
    }
    
    size_t file_size = (size_t)st_buf.st_size;
    char* file_buf = new char[file_size];
    if (!file_buf) {
        fprintf(stderr, "new buffer failed\n");
        return 1;
    }
    
    FILE* fp = fopen(filename, "rb");
    if (!fp) {
        fprintf(stderr, "open file failed: %s\n", filename);
        delete [] file_buf;
        return 1;
    }
    
    int ret = 1;
    if (fread(file_buf, 1, file_size, fp) == file_size) {
        content.append(file_buf, file_size);
        ret = 0;
    }
    
    delete [] file_buf;
    fclose(fp);
    return ret;
}

void run_as_daemon()
{
    pid_t pid = fork();
    if (pid == -1) {
        fprintf(stderr, "fork failed\n");
        return;
    } else if (pid > 0) {
        exit(0);
    }
    
    umask(0);
    setsid();
    
    // attach file descriptor 0, 1, 2 to "dev/null"
    int fd = open("/dev/null", O_RDWR, 0666);
    if (fd != -1) {
        dup2(fd, STDIN_FILENO);
        dup2(fd, STDOUT_FILENO);
        dup2(fd, STDERR_FILENO);
        
        if (fd > STDERR_FILENO) {
            close(fd);
        }
    }
}

vector<string> split(const string& str, const string& sep)
{
    vector<string> str_vector;
    string::size_type start_pos = 0, end_pos;
    while ((end_pos = str.find(sep, start_pos)) != string::npos) {
        if (end_pos != start_pos) {
            str_vector.push_back(str.substr(start_pos, end_pos - start_pos));
        }
        
        start_pos = end_pos + sep.size();
    };
    
    if (start_pos < str.size()) {
        str_vector.push_back(str.substr(start_pos, string::npos));
    }
    
    return str_vector;
}

void create_path(string& path)
{
    size_t len = path.size();
    if (path.at(len - 1) != '/') {
        path.append(1, '/');
    }
    
    // create path if not exist
    string parent_path;
    struct stat stat_buf;
    for (size_t i = 0; i < path.size(); ++i) {
        char ch = path.at(i);
        parent_path.append(1, ch);
        
        if ((ch == '/') && (stat(parent_path.c_str(), &stat_buf) != 0) && (mkdir(parent_path.c_str(), 0755) != 0)) {
            printf("create path failed: %s\n", parent_path.c_str());
            assert(false);
        }
    }
}

static void print_help(const char* program_name)
{
    printf("%s [-hv] [-c config_file]\n", program_name);
    printf("\t -h  :show this help message\n");
    printf("\t -v  :show version\n");
    printf("\t -c config_file  :specify configuration file\n");
}

void parse_command_args(int argc, char* argv[], const char* version, string& config_file)
{
    int ch;
    while ((ch = getopt(argc, argv, "hvc:")) != -1) {
        switch (ch) {
            case 'h':
                print_help(argv[0]);
                exit(0);
            case 'v':
                printf("Server Version: %s\n", version);
                printf("Server Build: %s %s\n", __DATE__, __TIME__);
                exit(0);
            case 'c':
                config_file = optarg;
                break;
            case '?':
            default:
                print_help(argv[0]);
                exit(1);
        }
    }
}

int get_long_from_string(const string& str, long& l)
{
    try {
        size_t idx;
        l = std::stol(str, &idx);
        if (idx != str.size()) {
            return CODE_ERROR;
        }
        
        return CODE_OK;
    } catch (std::exception& ex) {
        return CODE_ERROR;
    }
}

int get_ulong_from_string(const string& str, unsigned long& ul)
{
    try {
        size_t idx;
        ul = std::stoul(str, &idx);
        if (idx != str.size()) {
            return CODE_ERROR;
        }
        
        return CODE_OK;
    } catch (std::exception& ex) {
        return CODE_ERROR;
    }
}

int get_double_from_string(const string& str, double& d)
{
    try {
        size_t idx;
        d = std::stod(str, &idx);
        if (isnan(d) || (idx != str.size())) {
            return CODE_ERROR;
        }
        
        return CODE_OK;
    } catch (std::exception& ex) {
        return CODE_ERROR;
    }
}

uint64_t double_to_uint64(double d)
{
    uchar_t* p = (uchar_t*)&d;
    uint64_t u = *(uint64_t*)p;
    
    if (d >= 0) {
        u |= 0x8000000000000000;
    } else {
        u = ~u;
    }
    
    return u;
}

double uint64_to_double(uint64_t u)
{
    if ((u & 0x8000000000000000) > 0) {
        u &= ~0x8000000000000000;
    } else {
        u = ~u;
    }
    
    uchar_t* p = (uchar_t*)&u;
    return *(double*)p;
}

bool get_ip_port(const string& addr, string& ip, int& port)
{
    vector<string> ip_port_vec = split(addr, ":");
    if (ip_port_vec.size() != 2) {
        return false;
    }
    
    ip = ip_port_vec[0];
    port = atoi(ip_port_vec[1].c_str());
    if (!is_valid_ip(ip.c_str())) {
        return false;
    }
    
    if ((port < 1024) || (port > 0xFFFF)) {
        return false;
    }
    
    // remove some case like ip::port, :ip:port
    string merge_addr = ip + ":" + to_string(port);
    if (addr != merge_addr) {
        return false;
    }
    
    return true;
}
