def once(fn):
    context = {
        'ran': False,
    }

    def wrapped(*args, **kwargs):
        if context['ran']: # turn the second call and onwards into noop
            return
        context['ran'] = True

        fn(*args, **kwargs)

    return wrapped
