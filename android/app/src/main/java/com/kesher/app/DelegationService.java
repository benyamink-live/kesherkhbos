package com.kesher.app;

import com.google.androidbrowserhelper.locationdelegation.LocationDelegationExtraCommandHandler;

/** מאפשר ל-Chrome לבקש מיקום והתראות בשם האפליקציה */
public class DelegationService extends com.google.androidbrowserhelper.trusted.DelegationService {
    @Override
    public void onCreate() {
        super.onCreate();
        registerExtraCommandHandler(new LocationDelegationExtraCommandHandler());
    }
}
