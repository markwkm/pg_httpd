/* -------------------------------------------------------------------------
 *
 * pg_httpd.c
 *		HTTP daemon.
 *
 * Try to be an HTTP interface.
 *
 * Copyright (C) 2013, Mark Wong
 *
 * -------------------------------------------------------------------------
 */

#include "postgres.h"

/* These are always necessary for a bgworker */
#include "miscadmin.h"
#include "postmaster/bgworker.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "storage/lwlock.h"
#include "storage/proc.h"
#include "storage/shmem.h"

/* these headers are used by this particular worker's code */
#include "lib/stringinfo.h"
#include "pgstat.h"
#include "tcop/utility.h"

#define DEFAULT_PG_HTTPD_PORT 8888

PG_MODULE_MAGIC;

void _PG_init(void);
void pg_httpd_main(Datum);

/* flags set by signal handlers */
static volatile sig_atomic_t got_sighup = false;
static volatile sig_atomic_t got_sigterm = false;

/* GUC variables */
static int pg_httpd_port = DEFAULT_PG_HTTPD_PORT;

/*
 * Signal handler for SIGTERM
 *		Set a flag to let the main loop to terminate, and set our latch to wake
 *		it up.
 */
static void
pg_httpd_sigterm(SIGNAL_ARGS)
{
	int save_errno = errno;

	got_sigterm = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);

	errno = save_errno;
}

/*
 * Signal handler for SIGHUP
 *		Set a flag to let the main loop to reread the config file, and set
 *		our latch to wake it up.
 */
static void
pg_httpd_sighup(SIGNAL_ARGS)
{
	got_sighup = true;
	if (MyProc)
		SetLatch(&MyProc->procLatch);
}

void
pg_httpd_main(Datum main_arg)
{
	struct sockaddr_in sa;
	int val;
	int socket_listener;

	StringInfoData reply;

	/* Establish signal handlers before unblocking signals. */
	pqsignal(SIGHUP, pg_httpd_sighup);
	pqsignal(SIGTERM, pg_httpd_sigterm);

	/* We're now ready to receive signals */
	BackgroundWorkerUnblockSignals();

	/* Open a socket for incoming HTTP connections. */
	val= 1;

	memset(&sa, 0, sizeof(struct sockaddr_in));
	sa.sin_family = AF_INET;
	sa.sin_addr.s_addr = INADDR_ANY;
	sa.sin_port = htons((unsigned short) pg_httpd_port);

	socket_listener = socket(PF_INET, SOCK_STREAM, 0);
	if (socket_listener < 0)
	{
		elog(ERROR, "socket() error");
		proc_exit(1);
	}

	setsockopt(socket_listener, SOL_SOCKET, SO_REUSEADDR, &val, sizeof (val));

	if (bind(socket_listener, (struct sockaddr *) &sa,
			sizeof(struct sockaddr_in)) < 0)
	{
		elog(ERROR, "bind() error");
		proc_exit(1);
	}

	if (listen(socket_listener, 1) < 0)
	{
		elog(ERROR, "listen() error");
		proc_exit(1);
	}

	/*
	 * Main loop: do this until the SIGTERM handler tells us to terminate
	 */
	initStringInfo(&reply);
	while (!got_sigterm)
	{
		int rc;

		/* Socket stuff */
		int sockfd;
		socklen_t addrlen = sizeof(struct sockaddr_in);
		int received;
		int length = 2048;
		char data[length];
		int count;

		/* HTTP stuff */
		int content_length;

		/*
		 * Background workers mustn't call usleep() or any direct equivalent:
		 * instead, they may wait on their process latch, which sleeps as
		 * necessary, but is awakened if postmaster dies.  That way the
		 * background process goes away immediately in an emergency.
		 */
		rc = WaitLatch(&MyProc->procLatch,
				WL_LATCH_SET | WL_TIMEOUT | WL_POSTMASTER_DEATH,
				0);
		ResetLatch(&MyProc->procLatch);

		/* emergency bailout if postmaster has died */
		if (rc & WL_POSTMASTER_DEATH)
			proc_exit(1);

		/*
		 * In case of a SIGHUP, just reload the configuration.
		 */
		if (got_sighup)
		{
			got_sighup = false;
			ProcessConfigFile(PGC_SIGHUP);
		}

		sockfd = accept(socket_listener, (struct sockaddr *) &sa, &addrlen);
		if (sockfd == -1)
		{
			elog(WARNING, "accept() error");
			continue;
		}

		memset(data, 0, sizeof(data));
		received = recv(sockfd, data, length, 0);

		content_length = 12;
		appendStringInfo(&reply,
				"HTTP/1.0 200 OK\r\n" \
				"Content-Length: %d\r\n\r\n" \
				"Hello world!",
				content_length);
		send(sockfd, reply.data, strlen(reply.data), 0);
		close(sockfd);
		resetStringInfo(&reply);
	}

	proc_exit(1);
}

/*
 * Entry point of this module.
 *
 * We register more than one worker process here, to demonstrate how that can
 * be done.
 */
void
_PG_init(void)
{
	BackgroundWorker worker;

	/* get the configuration */
	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomIntVariable("pg_httpd.port",
			"HTTPD listener port.",
			NULL,
			&pg_httpd_port,
			DEFAULT_PG_HTTPD_PORT,
			1,
			65535,
			PGC_POSTMASTER,
			0,
			NULL,
			NULL,
			NULL);

	/* set up common data for all our workers */
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS |
			BGWORKER_BACKEND_DATABASE_CONNECTION;
	worker.bgw_start_time = BgWorkerStart_RecoveryFinished;
	worker.bgw_restart_time = BGW_NEVER_RESTART;
	worker.bgw_main = pg_httpd_main;
	worker.bgw_sighup = pg_httpd_sighup;
	worker.bgw_sigterm = pg_httpd_sigterm;
	worker.bgw_name = "pg_httpd";

	RegisterBackgroundWorker(&worker);
}
