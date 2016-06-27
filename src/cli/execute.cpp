// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <iostream>
#include <vector>

#include <mesos/resources.hpp>
#include <mesos/scheduler.hpp>
#include <mesos/type_utils.hpp>

#include <process/future.hpp>
#include <process/owned.hpp>
#include <process/pid.hpp>

#include <stout/check.hpp>
#include <stout/flags.hpp>
#include <stout/foreach.hpp>
#include <stout/hashmap.hpp>
#include <stout/none.hpp>
#include <stout/option.hpp>
#include <stout/os.hpp>

#include "common/parse.hpp"
#include "common/protobuf_utils.hpp"

using namespace mesos;
using namespace mesos::internal;

using process::Future;
using process::Owned;
using process::UPID;

using std::cerr;
using std::cout;
using std::endl;
using std::string;
using std::vector;


class Flags : public flags::FlagsBase
{
public:
  Flags()
  {
    add(&master,
        "master",
        "Mesos master (e.g., IP1:PORT1)");

    add(&command,
        "command",
        "Shell command to launch");

    add(&resources,
        "resources",
        "Resources for the command",
        "cpus:1;mem:128");

    add(&name,
        "name",
        "The name of framework",
        "test");

    add(&role,
        "role",
        "The role of framework");

    add(&revocable,
        "revocable",
        "Accept revocable resources",
        false);
  }

  Option<string> master;
  Option<string> command;
  bool revocable;
  string resources;
  string name;
  Option<string> role;
};


class CommandScheduler : public Scheduler
{
public:
  CommandScheduler(
      const Option<string>& _command,
      const string& _resources,
      bool _revocable)
    : command(_command),
      resources(_resources),
      revocable(_revocable)
  {
    taskIndex = 0;
  }

  virtual ~CommandScheduler() {}

  virtual void registered(
      SchedulerDriver* _driver,
      const FrameworkID& _frameworkId,
      const MasterInfo& _masterInfo) {
    cout << "Framework registered with " << _frameworkId << endl;
  }

  virtual void reregistered(
      SchedulerDriver* _driver,
      const MasterInfo& _masterInfo) {
    cout << "Framework re-registered" << endl;
  }

  virtual void disconnected(
      SchedulerDriver* driver) {}

  virtual void resourceOffers(
      SchedulerDriver* driver,
      const vector<Offer>& offers)
  {
    static Try<Resources> TASK_RESOURCES = Resources::parse(resources);

    if (TASK_RESOURCES.isError()) {
      cerr << "Failed to parse resources '" << resources
           << "': " << TASK_RESOURCES.error() << endl;
      driver->abort();
      return;
    }

    if (revocable) {
        TASK_RESOURCES = TASK_RESOURCES.get()
            .flatten(Resource::RevocableInfo());
    }

    foreach (const Offer& offer, offers) {
      cout << "Received offer " << offer.id() << " from slave "
           << offer.slave_id() << " (" << offer.hostname() << ") "
           << "with " << offer.resources() << endl;

      if (Resources(offer.resources()).contains(TASK_RESOURCES.get())) {
        string name = string("Test_") + stringify(taskIndex++);
        TaskInfo task;
        task.set_name(name);
        task.mutable_task_id()->set_value(stringify(taskIndex));
        task.mutable_slave_id()->MergeFrom(offer.slave_id());
        task.mutable_resources()->CopyFrom(TASK_RESOURCES.get());

        CommandInfo* commandInfo = task.mutable_command();

        CHECK_SOME(command);

        commandInfo->set_shell(true);
        commandInfo->set_value(command.get());

        vector<TaskInfo> tasks;
        tasks.push_back(task);

        driver->launchTasks(offer.id(), tasks);
        cout << "task " << name << " submitted to slave "
             << offer.slave_id() << endl;
      } else {
        driver->declineOffer(offer.id());
      }
    }
  }

  virtual void offerRescinded(
      SchedulerDriver* driver,
      const OfferID& offerId) {}

  virtual void statusUpdate(
      SchedulerDriver* driver,
      const TaskStatus& status)
  {
    cout << "Received status update " << status.state()
         << " for task " << status.task_id() << endl;
  }

  virtual void frameworkMessage(
      SchedulerDriver* driver,
      const ExecutorID& executorId,
      const SlaveID& slaveId,
      const string& data) {}

  virtual void slaveLost(
      SchedulerDriver* driver,
      const SlaveID& sid) {}

  virtual void executorLost(
      SchedulerDriver* driver,
      const ExecutorID& executorID,
      const SlaveID& slaveID,
      int status) {}

  virtual void error(
      SchedulerDriver* driver,
      const string& message) {}

private:
  int taskIndex;
  const Option<string> command;
  const string resources;
  bool revocable;
};


int main(int argc, char** argv)
{
  Flags flags;

  // Load flags from environment and command line.
  Try<Nothing> load = flags.load(None(), argc, argv);

  if (load.isError()) {
    cerr << flags.usage(load.error()) << endl;
    return EXIT_FAILURE;
  }

  // TODO(marco): this should be encapsulated entirely into the
  // FlagsBase API - possibly with a 'guard' that prevents FlagsBase
  // from calling ::exit(EXIT_FAILURE) after calling usage() (which
  // would be the default behavior); see MESOS-2766.
  if (flags.help) {
    cout << flags.usage() << endl;
    return EXIT_SUCCESS;
  }

  if (flags.master.isNone()) {
    cerr << flags.usage("Missing required option --master") << endl;
    return EXIT_FAILURE;
  }

  UPID master("master@" + flags.master.get());
  if (!master) {
    cerr << flags.usage("Could not parse --master=" + flags.master.get())
         << endl;
    return EXIT_FAILURE;
  }

  if (flags.command.isNone()) {
    cerr << flags.usage("Missing required option --command") << endl;
    return EXIT_FAILURE;
  }

  Result<string> user = os::user();
  if (!user.isSome()) {
    if (user.isError()) {
      cerr << "Failed to get username: " << user.error() << endl;
    } else {
      cerr << "No username for uid " << ::getuid() << endl;
    }
    return EXIT_FAILURE;
  }

  CommandScheduler scheduler(
      flags.command,
      flags.resources,
      flags.revocable);

  FrameworkInfo framework;
  framework.set_user(user.get());
  framework.set_name(flags.name);
  if (flags.role.isSome()) {
    framework.set_role(flags.role.get());
  }
  framework.set_checkpoint(true);

  if (flags.revocable) {
    framework.add_capabilities()->set_type(
        FrameworkInfo::Capability::REVOCABLE_RESOURCES);
  }

  MesosSchedulerDriver driver(&scheduler, framework, flags.master.get());

  return driver.run() == DRIVER_STOPPED ? EXIT_SUCCESS : EXIT_FAILURE;
}
