from __future__ import print_function
import os, time

ERROR_LOG_FILENAME = 'failure_{}.log'.format(time.strftime('%m%d_%H%M'))


def apply_to(rootname, function, print_process=False, step=1000):
    bad_files = []
    count = [0, 0]
    tm = time.time()
    speed = 1
    for root, _, files in os.walk(rootname):
        for f in files:
            count[0] += 1
            filepath = os.path.abspath(root + '/' + f)
            if count[0] % step == 0:
                speed = (time.time() - tm) / float(step)
                tm = time.time()
            if print_process:
                print('\rfailure-rate:{:.6f}     {} | {:.5f}(s) | {}...      ' \
                      .format(len(bad_files) / float(count[0]), count, speed, filepath, end=''))
            try:
                failed = function(filepath)
                if not failed:
                    count[1] += 1
            except Exception as e:
                bad_files += [filepath]
                with open(ERROR_LOG_FILENAME, 'a') as f:
                    f.write(filepath + '  ' + str(e) + '\n')

    bd_str = '\n'.join(bad_files)
    if print_process:
        print('\nunsuccessful tries:\n{}'.format(bd_str))