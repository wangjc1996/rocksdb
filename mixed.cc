#include <unistd.h>
#include <getopt.h>
#include <cstdlib>
#include <cassert>
#include <cstring>
#include <cinttypes>
#include <string>
#include <thread>
#include <vector>
#include <algorithm>
#include <iostream>
#include <random>
#include <sstream>

#include "rocksdb/db.h"
#include "rocksdb/utilities/transaction.h"
#include "rocksdb/utilities/transaction_db.h"

using namespace std;
using namespace std::chrono;
using namespace rocksdb;

const string default_db_path = "./rocks_data/";
const string default_wal_path = "./rocks_data/wal";

static int runtime = 10;
volatile bool starting, running;


#define HAS_RETRY 1

static struct {
	int keys = 10000;
	int threads = 10;
	int reads = 25;
	int writes = 5;
	bool sync = false;
	bool has_conflict = true;
	int ratio_2pl = 1;
	int ratio_occ = 1;
} opt;

void initialize_db(TransactionDB** db_ptr, const string db_path, const string wal_path) {
	Options options;
	options.IncreaseParallelism();
	options.wal_dir = wal_path;
	options.manual_wal_flush = false;
	options.create_if_missing = true;
	options.write_buffer_size = 512 << 20;
	options.max_write_buffer_number = 10;

	TransactionDBOptions txn_db_options;
	txn_db_options.write_policy = TxnDBWritePolicy::WRITE_COMMITTED;

	Status status = TransactionDB::Open(options, txn_db_options, db_path, db_ptr);

	if (!status.ok()) printf("Error during initialization\n");
}

void do_prepare(TransactionDB* db, int keys) {
	Status s;
	WriteOptions writeOptions;
	writeOptions.sync = false;
	Transaction *txn = db->BeginTransaction(writeOptions, TransactionOptions());

	for (int i = 0; i < keys; ++i) {
		const char* key = to_string(i).c_str();
		const char* value = "*";

		s = txn->Put(key, value);
		if (!s.ok()) break;
	}

	if (s.ok()) {
		s = txn->Commit();
	}
	if (!s.ok()) printf("Error: %s\n", s.ToString().c_str());
	else printf("Done, %d keys inserted\n", keys);

	delete txn;
}

void timing_thread_func() {
	sleep(3);
	starting = true;
	sleep(runtime);
	running = false;
}

using my_clock = steady_clock;
using dur_t = my_clock::duration;
struct thread_data {
	vector<dur_t> sum_time;
	dur_t whole_txn_time = dur_t::zero();
	int64_t committed = 0;
	int64_t aborted = 0;
	int64_t num_req = 0;
};

void worker_thread_func(thread_data *data, int num_threads, int index, TransactionDB *db) {
	mt19937 gen;
	gen.seed(reinterpret_cast<intptr_t>(data));
	
	int txn_size = opt.reads + opt.writes;
	int rand_bound = opt.ratio_2pl + opt.ratio_occ;

        uniform_int_distribution<> dist;
	if (opt.has_conflict) {
		dist = uniform_int_distribution<>(0, opt.keys - 1);
	} else {
		int limit = opt.keys / num_threads;
		dist = uniform_int_distribution<>(index * limit, (index + 1) * limit - 1);
	}
	std::uniform_int_distribution<> dist_write(0, txn_size - 1);
	vector<int> write_pos_pool(txn_size);
	for (int i = 0; i < txn_size; i++) write_pos_pool[i] = i;

	Status s;
	bool succ;

	while (!starting) continue;

	while (running) {
		succ = false;

		vector<int> ids(txn_size);
		for (int i = 0; i < txn_size; ++i) ids[i] = dist(gen);
		// sort(ids.begin(), ids.end()); // control if there are deadlocks

		random_shuffle(write_pos_pool.begin(), write_pos_pool.end());
		vector<int> write_pos(write_pos_pool.begin(), write_pos_pool.begin() + opt.writes);
		sort(write_pos.begin(), write_pos.end());

		auto tt0 = my_clock::now();
#if HAS_RETRY
		while (!succ && running) {
#endif
			succ = true;

			WriteOptions writeOptions;
			writeOptions.sync = opt.sync;
			TransactionOptions txn_option;
			// txn_option.deadlock_detect = false; // control if there are deadlock detection

			Transaction *txn = db->BeginTransaction(writeOptions, txn_option);
			auto iter = write_pos.begin();
			auto guard = write_pos.end();
			ReadOptions readOptions;

			for (int i = 0; i < txn_size; ++i) {
				bool optimistic = (rand() % rand_bound) < opt.ratio_occ;

				string id = to_string(ids[i]);
				const char *key = id.c_str();

				auto t0 = my_clock::now();
				if (iter != guard && *iter == i) {
					s = txn->DoPut(key, id.c_str(), optimistic);
					//s = txn->Put(key, id.c_str());
					iter++;
				} else {
					string tmp_string;
					s = txn->DoGet(readOptions, id.c_str(), &tmp_string, optimistic);
					//s = txn->Get(readOptions, id.c_str(), &tmp_string);
					//printf("%s\n", tmp_string.c_str());
				}
				auto t1 = my_clock::now();

				data->sum_time[i] +=  t1 - t0;
				if (!s.ok()) {
					succ = false;
					break;
				} else {
					++data->num_req;
				}
			}

			auto t0 = my_clock::now();
			// db->ReleaseSnapshot(readOptions.snapshot);
			if (succ) s = txn->Commit();
			else s = txn->Rollback();
			auto t1 = my_clock::now();

			if (!s.ok()) succ = false;

			if (succ) {
				//data->sum_time[txn_size] = data->sum_time[txn_size] + t1 - t0;
				data->sum_time[txn_size] +=  t1 - t0;
				++data->committed;
			} else {
				data->sum_time[txn_size] +=  t1 - t0;
				++data->aborted;
			}
			delete txn;
#if HAS_RETRY
		}
#endif
		auto tt1 = my_clock::now();

		// succ may be false for no-retry case
		if (succ) data->whole_txn_time += tt1 - tt0;
	}

}

void do_run(TransactionDB *db) {
	double percent_2pl = ((double)opt.ratio_2pl) / (opt.ratio_2pl + opt.ratio_occ);

	//printf("threads: %d, key range: %d\nreads: %d, writes: %d\nsync: %s, has conflict: %s\n2pl%%: %.2lf, occ%%: %.2lf\n", opt.threads, opt.keys, opt.reads, opt.writes, (opt.sync ? "true" : "false"), (opt.has_conflict ? "true" : "false"), percent_2pl * 100, (1 - percent_2pl) * 100);
	starting = false;
	running = true;

	std::thread timing_thread(timing_thread_func);
	std::vector<std::thread> worker_threads;

	int threads = opt.threads;
	int txn_size = opt.reads + opt.writes;
	thread_data* thread_data_arr = new thread_data[threads];
	for (int i = 0; i < threads; ++i) {
		thread_data_arr[i].sum_time = vector<dur_t>(txn_size + 1);
		worker_threads.emplace_back(worker_thread_func, &thread_data_arr[i], threads, i, db);
	}
	timing_thread.join();

	thread_data agg;
	dur_t non_commit_lat(0);
	dur_t commit_lat(0);
	for (int i = 0; i < threads; ++i) {
		worker_threads[i].join();
		agg.committed += thread_data_arr[i].committed;
		agg.aborted += thread_data_arr[i].aborted;
		agg.num_req += thread_data_arr[i].num_req;
		agg.whole_txn_time += thread_data_arr[i].whole_txn_time;
		for (int j = 0; j < txn_size; ++j) {
			non_commit_lat += thread_data_arr[i].sum_time[j];
			// printf("sum time %d = %zd acc = %zd\n", j, thread_data_arr[i].sum_time[j].count(), non_commit_lat.count());
		}
		commit_lat += thread_data_arr[i].sum_time[txn_size];
		// printf("commit time %d = %zd\n", txn_size, thread_data_arr[i].sum_time[txn_size].count());
	}

	//cout << "agg commit thpt: " << 1.0 * agg.committed / runtime << "tps" << endl;
	//cout << "agg abort thpt: " << 1.0 * agg.aborted / runtime << "tps" << endl;
	printf("%.2lf\t%.2lf\n", 1.0 * agg.committed / runtime, 1.0 * agg.aborted / runtime);
	//cout << "non commit lat: " << duration_cast<microseconds>(non_commit_lat / (agg.num_req)).count() << endl;
	//cout << "commit lat: " << duration_cast<microseconds>(commit_lat / agg.committed).count() << endl;
	//cout << "whole txn lat: " << duration_cast<microseconds>(agg.whole_txn_time / agg.committed).count() << endl;

	delete[] thread_data_arr;
}


static const struct option prepare_long_opts[] = {
    { "wal_path", required_argument, NULL, 3 },
    { "db_path", required_argument, NULL, 4 },
};

static const struct option run_long_opts[] = {
    { "2pl", required_argument, NULL, 1 },
    { "occ", required_argument, NULL, 2 },
    { "walpath", required_argument, NULL, 3 },
    { "dbpath", required_argument, NULL, 4 },
};

const char* prepare_optstr = "k:";
const char* run_optstr = "t:k:r:w:c:s:";

bool get_num_opt(int *val, const char* err_msg, bool allow_zero = false) {
	int number = strtol(optarg, nullptr, 0);
	if (number <= 0) {
		if (number != 0 || !allow_zero) {
			if (err_msg != nullptr) {
				printf("%s\n", err_msg);
			}
			return false;
		}
	}
	*val = number;
	return true;
}

bool processs_run_args(int argc, char *argv[], string& db_path, string& wal_path) {
	int optc;
	while(-1 != (optc = getopt_long(argc, argv, run_optstr, run_long_opts, nullptr))) {
		switch(optc) {
			case 't':
				if (!get_num_opt(&opt.threads, "number of threads should be positive")) {
					return false;
				}
				break;
			case 'k':
				if (!get_num_opt(&opt.keys, "number of keys should be positive")) {
					return false;
				}
				break;
			case 'r':
				if (!get_num_opt(&opt.reads, "number of reads should be positive", true)) {
					return false;
				}
				break;
			case 'w':
				if (!get_num_opt(&opt.writes, "number of writes should be positive", true)) {
					return false;
				}
				break;
			case 'c':
				if (strcmp("false", optarg) == 0 || strcmp("no", optarg) == 0 || strcmp("0", optarg) ==  0) {
					opt.has_conflict = false;
				} else {
					opt.has_conflict = true;
				}
				break;
			case 's':
				if (strcmp("false", optarg) == 0 || strcmp("no", optarg) == 0 || strcmp("0", optarg) ==  0) {
					opt.sync = false;
				} else {
					opt.sync = true;
				}
				break;
			case 1:	
				if (!get_num_opt(&opt.ratio_2pl, "ratio of 2pl should be positive", true)) {
					return false;
				}
				break;
			case 2:	
				if (!get_num_opt(&opt.ratio_occ, "number of occ should be positive", true)) {
					return false;
				}
				break;
			case 3:
				db_path = optarg;
				break;
			case 4:
				wal_path = optarg;
				break;
			default:
				printf("warning: unknown arg");
		}
	}
	return true;
}

int main(int argc, char *argv[]) {
	const string command = argv[1];

	string db_path = default_db_path;
	string wal_path = default_wal_path;

	if (command == "prepare") {
		int optc;
		int keys = 100000;
		while(-1 != (optc = getopt_long(argc, argv, prepare_optstr, prepare_long_opts, nullptr))) {
			switch(optc) {
				case 3:
					db_path = optarg;
					break;
				case 4:
					wal_path = optarg;
					break;
				case 'k':
					if (!get_num_opt(&keys, "number of keys should be positive")) {
						return -1;
					}
					break;
				default:
					printf("warning: unknown arg");
			}
		}
		TransactionDB* db;
		initialize_db(&db, db_path, wal_path);
		do_prepare(db, keys);
		return 0;
	} 

	if (command == "run") {
		if (!processs_run_args(argc, argv, db_path, wal_path)) {
			return -1;
		}
		if (opt.reads + opt.writes == 0) {
			printf("workload should not be empty\n");
			return -1;
		}

		if (opt.ratio_2pl + opt.ratio_occ == 0) {
			printf("invalid occ & 2pl ratio");
			return -1;
		}

		TransactionDB* db;
		initialize_db(&db, db_path, wal_path);
		do_run(db);
		return 0;
	} 

	printf("please specify target\n");
	return -1;
}
